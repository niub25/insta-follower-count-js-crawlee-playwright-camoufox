import { Actor, log } from 'apify';
import { Dataset } from 'crawlee';
import { gotScraping } from 'got-scraping';

await Actor.init();

// ─── Input ────────────────────────────────────────────────────────────────────

const input = await Actor.getInput();
const {
    usernames: inputUsernames = [],
    sessions:  inputSessions  = [],
    concurrency               = 5,
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

log.info(`Sessions: ${inputSessions.length} | Restored: ${doneUsernames.size} done | Pending: ${pendingQueue.length} | Total: ${inputUsernames.length} | Concurrency: ${concurrency}`);

Actor.on('migrating', async () => {
    await Actor.setValue('STATE', { doneUsernames: [...doneUsernames] });
    log.info(`[MIGRATION] Saved — ${doneUsernames.size} done`);
});

// ─── Session pool ─────────────────────────────────────────────────────────────
// Each session = one Instagram account.
// Picks the least-used session that isn't cooling down.
// On 429/block: escalating cooldown (60s → 120s → 300s → 600s).
// Resets consecutive fail count on any success.

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

// ─── HTTP fetch via got-scraping ──────────────────────────────────────────────
// got-scraping handles TLS fingerprinting at the network level — the same
// library used by Crawlee's CheerioCrawler which powers the official working
// Apify Instagram actor.
// Playwright's ctx.request.get() has Firefox-specific cookie handling quirks
// that cause redirect loops on Instagram's API. got-scraping avoids this entirely.

const IG_UA = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1';

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

        try {
            const proxyUrl  = await proxyConfig.newUrl(`s${session.id}`);

            const response = await gotScraping({
                url,
                method:          'GET',
                headers: {
                    ...IG_HEADERS,
                    'Referer': `https://www.instagram.com/${username}/`,
                    'Cookie':  `sessionid=${session.sessionId}${session.csrfToken ? `; csrftoken=${session.csrfToken}` : ''}`,
                    ...(session.csrfToken ? { 'X-CSRFToken': session.csrfToken } : {}),
                },
                proxyUrl,
                responseType:    'json',
                timeout:         { request: 15_000 },
                throwHttpErrors: false,
                retry:           { limit: 0 },
                followRedirect:  false,   // A redirect = not authenticated; don't follow
            });

            const status = response.statusCode;

            if (status === 200) {
                const user = response.body?.data?.user;
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

            // Any block / auth failure / redirect → rotate session
            if (status === 429 || status === 401 || status === 403 || (status >= 300 && status < 400)) {
                sessionPool.onBlock(session);
                continue;
            }

            log.warning(`[${username}] HTTP ${status} on session #${session.id}`);
            sessionPool.onError(session);

        } catch (e) {
            log.warning(`[${username}] Error: ${e.message?.split('\n')[0]}`);
            sessionPool.onError(session);
        }

        await new Promise(r => setTimeout(r, 500));
    }

    return { found: false, followers: null, source: 'all_attempts_failed' };
}

// ─── Smoke test ───────────────────────────────────────────────────────────────

log.info('Running smoke test...');
const smoke = await fetchFollowers('instagram');

if (smoke.found) {
    log.info(`Smoke test OK — @instagram has ${smoke.followers?.toLocaleString()} followers [${smoke.source}]`);
} else if (smoke.source === 'all_sessions_dead') {
    log.error([
        'ABORT — All sessions are blocked.',
        'Refresh sessionId + csrfToken from Chrome DevTools for each account and re-run.',
    ].join('\n'));
    await Actor.exit();
} else {
    log.warning(`Smoke test [${smoke.source}] — proceeding`);
}

// ─── Concurrent batch processor ───────────────────────────────────────────────

const total     = pendingQueue.length;
let   succeeded = 0;
let   failed    = 0;
let   processed = 0;

log.info(`${'─'.repeat(55)}\nProcessing ${total} usernames | concurrency: ${concurrency}\n${'─'.repeat(55)}`);

async function processOne(username, globalIndex) {
    const progress              = `[${globalIndex + 1 + doneUsernames.size}/${total + doneUsernames.size}]`;
    const { found, followers, source } = await fetchFollowers(username);

    await Dataset.pushData({ username, followers: followers ?? null, scrapedAt: new Date().toISOString() });
    doneUsernames.add(username);
    processed++;

    if (found && followers !== null) {
        succeeded++;
        log.info(`${progress} @${username.padEnd(30)} → ${String(followers.toLocaleString()).padStart(10)} followers  [${source}]`);
    } else {
        failed++;
        log.info(`${progress} @${username.padEnd(30)} → failed  [${source}]`);
    }
}

for (let i = 0; i < pendingQueue.length; i += concurrency) {
    const chunk = pendingQueue.slice(i, i + concurrency);
    await Promise.all(chunk.map((username, j) => processOne(username, i + j)));

    if (processed % 200 < concurrency || i + concurrency >= pendingQueue.length) {
        await Actor.setValue('STATE', { doneUsernames: [...doneUsernames] });
        log.info(`[checkpoint] ${processed} done | ${sessionPool.stats()}`);
    }
}

// ─── Teardown ─────────────────────────────────────────────────────────────────

await Actor.setValue('STATE', { doneUsernames: [...doneUsernames] });
log.info(`${'═'.repeat(55)}\nDONE — Processed: ${doneUsernames.size} | Succeeded: ${succeeded} | Failed: ${failed}\nSessions: ${sessionPool.stats()}\n${'═'.repeat(55)}`);
await Actor.exit();
