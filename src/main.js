import { Actor, log } from 'apify';
import { Dataset } from 'crawlee';
import { launchOptions } from 'camoufox-js';
import { firefox } from 'playwright-core';

await Actor.init();

// ─── Input ────────────────────────────────────────────────────────────────────

const input = await Actor.getInput();
const {
    usernames: inputUsernames = [],
    sessions:  inputSessions  = [],
    proxyConfiguration,
} = input;

if (!inputUsernames?.length) {
    log.error('usernames array is empty.');
    await Actor.exit();
}
if (!inputSessions?.length) {
    log.error('sessions array is empty. Add at least one Instagram session.');
    await Actor.exit();
}

const proxyConfig = await Actor.createProxyConfiguration(
    proxyConfiguration ?? {
        useApifyProxy:     true,
        apifyProxyGroups:  ['RESIDENTIAL'],
        apifyProxyCountry: 'US',
    }
);

// ─── Restore state ────────────────────────────────────────────────────────────

const savedState    = await Actor.getValue('STATE') ?? {};
const doneUsernames = new Set(savedState.doneUsernames ?? []);

const seenDedup = new Set();
const pendingQueue = inputUsernames
    .map(u => u.trim().replace(/^@/, '').toLowerCase())
    .filter(u => {
        if (!u || doneUsernames.has(u) || seenDedup.has(u)) return false;
        seenDedup.add(u);
        return true;
    });

log.info(`Sessions: ${inputSessions.length} | Restored: ${doneUsernames.size} done | Pending: ${pendingQueue.length} | Total: ${inputUsernames.length}`);

Actor.on('migrating', async () => {
    await Actor.setValue('STATE', { doneUsernames: [...doneUsernames] });
    log.info(`[MIGRATION] Saved — ${doneUsernames.size} done`);
});

// ─── Session pool ─────────────────────────────────────────────────────────────
// Each session = one Instagram account.
// Rotation: always pick the least-used session that isn't in cooldown.
// On 429/block: escalating cooldown (60s → 120s → 300s → 600s).
// Camoufox handles browser-level fingerprinting; the pool handles account-level
// rate limiting across multiple Instagram accounts.

class SessionPool {
    constructor(sessions) {
        this.pool = sessions.map((s, i) => ({
            id:               i,
            sessionId:        s.sessionId,
            csrfToken:        s.csrfToken || '',
            requests:         0,
            consecutiveFails: 0,
            cooldownUntil:    0,
        }));
    }

    acquire() {
        const now       = Date.now();
        const available = this.pool
            .filter(s => s.cooldownUntil <= now && s.consecutiveFails < 5)
            .sort((a, b) => a.requests - b.requests);
        return available[0] ?? null;
    }

    msUntilNextAvailable() {
        const now     = Date.now();
        const soonest = this.pool
            .filter(s => s.cooldownUntil > now && s.consecutiveFails < 5)
            .sort((a, b) => a.cooldownUntil - b.cooldownUntil)[0];
        return soonest ? Math.max(0, soonest.cooldownUntil - now) : null;
    }

    onSuccess(session) {
        session.requests++;
        session.consecutiveFails = Math.max(0, session.consecutiveFails - 1);
    }

    onBlock(session) {
        session.consecutiveFails++;
        const cooldownMs      = Math.min(60_000 * session.consecutiveFails, 600_000);
        session.cooldownUntil = Date.now() + cooldownMs;
        log.warning(`Session #${session.id} blocked (${session.consecutiveFails}×) — cooldown ${cooldownMs / 1000}s`);
    }

    onError(session) {
        session.consecutiveFails++;
    }

    allDead() {
        return this.pool.every(s => s.consecutiveFails >= 5);
    }

    stats() {
        return this.pool.map(s => {
            const cd = Math.max(0, s.cooldownUntil - Date.now());
            return `#${s.id}[${s.requests}req,${cd > 0 ? `cd${Math.ceil(cd / 1000)}s` : 'ok'}]`;
        }).join(' ');
    }
}

const sessionPool = new SessionPool(inputSessions);

// ─── Launch Camoufox ──────────────────────────────────────────────────────────
// Camoufox is a stealthy Firefox fork that patches the browser fingerprinting
// APIs Instagram uses to detect headless scrapers:
//   - navigator.webdriver → patched to undefined
//   - Canvas/WebGL noise → randomised per session
//   - TLS fingerprint     → real Firefox JA3/JA4 signature
//   - Timing APIs         → human-like jitter added
//
// We launch ONE Camoufox instance and create one BrowserContext per session.
// context.request.get() on each context sends the right cookies automatically
// and goes through Firefox's real TLS stack — not Chromium's.

log.info('Launching Camoufox (stealthy Firefox)...');

const proxyUrl  = await proxyConfig.newUrl('camoufox_main');
const proxyHost = proxyUrl ? new URL(proxyUrl) : null;

const camoufoxOpts = await launchOptions({
    os:       'macos',   // Spoof macOS fingerprint — most common Instagram user OS
    headless: true,
});

const browser = await firefox.launch({
    ...camoufoxOpts,
    proxy: proxyHost
        ? {
              server:   `${proxyHost.protocol}//${proxyHost.host}`,
              username: proxyHost.username ? decodeURIComponent(proxyHost.username) : undefined,
              password: proxyHost.password ? decodeURIComponent(proxyHost.password) : undefined,
          }
        : undefined,
});

log.info(`Creating ${inputSessions.length} browser context(s)...`);

const IG_UA = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1';

const contexts = await Promise.all(
    inputSessions.map(async (session, i) => {
        const ctx = await browser.newContext({
            userAgent: IG_UA,
            viewport:  { width: 390, height: 844 },
        });
        await ctx.addCookies([
            { name: 'sessionid', value: session.sessionId, domain: '.instagram.com', path: '/', httpOnly: true, secure: true },
            ...(session.csrfToken
                ? [{ name: 'csrftoken', value: session.csrfToken, domain: '.instagram.com', path: '/', secure: true }]
                : []),
        ]);
        log.info(`Context #${i} ready`);
        return ctx;
    })
);

// ─── Fetch follower count ─────────────────────────────────────────────────────
// Uses context.request.get() — Playwright's native HTTP client that:
//   1. Reads cookies from the BrowserContext (our injected sessionId)
//   2. Uses Camoufox's Firefox TLS fingerprint at the network level
//   3. Is faster than page navigation (no HTML/JS/CSS rendering)
//
// On block: rotates to the next session's context automatically.

const IG_HEADERS = {
    'X-IG-App-ID':      '936619743392459',
    'X-ASBD-ID':        '129477',
    'X-IG-WWW-Claim':   '0',
    'Accept':           '*/*',
    'Accept-Language':  'en-US,en;q=0.9',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent':       IG_UA,
};

async function fetchFollowers(username) {
    const url = `https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`;

    // Try each session up to (sessions + 1) times
    for (let attempt = 0; attempt < inputSessions.length + 2; attempt++) {
        // Wait for an available session
        let session = sessionPool.acquire();
        while (!session) {
            if (sessionPool.allDead()) {
                return { found: false, followers: null, source: 'all_sessions_dead' };
            }
            const wait = sessionPool.msUntilNextAvailable() ?? 30_000;
            log.warning(`All sessions cooling — waiting ${Math.ceil(wait / 1000)}s`);
            await new Promise(r => setTimeout(r, wait + 1_000));
            session = sessionPool.acquire();
        }

        const ctx = contexts[session.id];

        try {
            const response = await ctx.request.get(url, {
                headers: {
                    ...IG_HEADERS,
                    'Referer': `https://www.instagram.com/${username}/`,
                    // Pass Cookie header explicitly — Firefox's ctx.request drops
                    // cookies on redirects (unlike Chromium), causing a redirect loop.
                    // Explicit header ensures cookies survive every hop.
                    'Cookie': `sessionid=${session.sessionId}${session.csrfToken ? `; csrftoken=${session.csrfToken}` : ''}`,
                    ...(session.csrfToken ? { 'X-CSRFToken': session.csrfToken } : {}),
                },
                timeout:      15_000,
                maxRedirects: 0,  // Never follow redirects — a redirect = not authenticated
            });

            const status = response.status();

            if (status === 200) {
                const body = await response.json();
                const user = body?.data?.user;
                if (!user) {
                    sessionPool.onSuccess(session);
                    return { found: false, followers: null, source: 'no_user' };
                }
                sessionPool.onSuccess(session);
                return {
                    found:     true,
                    followers: user.edge_followed_by?.count ?? user.follower_count ?? null,
                    source:    `s${session.id}`,
                };
            }

            if (status === 404) {
                sessionPool.onSuccess(session);
                return { found: false, followers: null, source: 'not_found' };
            }

            if (status === 429 || status === 401 || status === 403) {
                sessionPool.onBlock(session);
                continue; // rotate to next session
            }

            // 3xx redirect = Instagram is not recognising this session as authenticated
            if (status >= 300 && status < 400) {
                log.warning(`Session #${session.id} got redirect (${status}) — treating as auth failure`);
                sessionPool.onBlock(session);
                continue;
            }

            log.warning(`[${username}] HTTP ${status} on session #${session.id}`);
            sessionPool.onError(session);

        } catch (e) {
            log.warning(`[${username}] Error on session #${session.id}: ${e.message.split('\n')[0]}`);
            sessionPool.onError(session);
        }

        await new Promise(r => setTimeout(r, 500));
    }

    return { found: false, followers: null, source: 'all_attempts_failed' };
}

// ─── Smoke test ───────────────────────────────────────────────────────────────

log.info('Running smoke test on @instagram...');
const smoke = await fetchFollowers('instagram');

if (smoke.found) {
    log.info(`Smoke test OK — @instagram has ${smoke.followers?.toLocaleString()} followers [${smoke.source}]`);
} else if (smoke.source === 'all_sessions_dead') {
    log.error([
        'ABORT — All sessions are blocked or returning redirects.',
        'This usually means sessionId cookies are expired.',
        'Get fresh sessionId + csrfToken from Chrome DevTools for each account and re-run.',
    ].join('\n'));
    await browser.close();
    await Actor.exit();
} else {
    log.warning(`Smoke test: [${smoke.source}] — proceeding anyway`);
}

// ─── Main loop ────────────────────────────────────────────────────────────────
// Sequential with a small delay — Camoufox's stealth + session rotation means
// we don't need to rush. Steady pace avoids triggering rate limits.

const total     = pendingQueue.length;
let   succeeded = 0;
let   failed    = 0;

log.info(`${'─'.repeat(55)}\nProcessing ${total} usernames\n${'─'.repeat(55)}`);

for (let i = 0; i < pendingQueue.length; i++) {
    const username = pendingQueue[i];
    const progress = `[${i + 1 + doneUsernames.size}/${total + doneUsernames.size}]`;

    const { found, followers, source } = await fetchFollowers(username);

    await Dataset.pushData({
        username,
        followers: followers ?? null,
        scrapedAt: new Date().toISOString(),
    });
    doneUsernames.add(username);

    if (found && followers !== null) {
        succeeded++;
        log.info(`${progress} @${username.padEnd(30)} → ${String(followers.toLocaleString()).padStart(10)} followers  [${source}]`);
    } else {
        failed++;
        log.info(`${progress} @${username.padEnd(30)} → failed  [${source}]`);
    }

    // Checkpoint + session stats every 200 profiles
    if ((i + 1) % 200 === 0 || i + 1 === pendingQueue.length) {
        await Actor.setValue('STATE', { doneUsernames: [...doneUsernames] });
        log.info(`[checkpoint] ${i + 1} done | ${sessionPool.stats()}`);
    }

    await new Promise(r => setTimeout(r, 600));
}

// ─── Teardown ─────────────────────────────────────────────────────────────────

await Promise.all(contexts.map(ctx => ctx.close()));
await browser.close();
await Actor.setValue('STATE', { doneUsernames: [...doneUsernames] });

log.info([
    `${'═'.repeat(55)}`,
    `DONE`,
    `  Processed : ${doneUsernames.size}`,
    `  Succeeded  : ${succeeded}`,
    `  Failed     : ${failed}`,
    `  Sessions   : ${sessionPool.stats()}`,
    `${'═'.repeat(55)}`,
].join('\n'));

await Actor.exit();
