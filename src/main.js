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

if (!inputUsernames?.length) { log.error('usernames array is empty.'); await Actor.exit(); }
if (!inputSessions?.length)  { log.error('sessions array is empty.'); await Actor.exit(); }

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

log.info(`Sessions: ${inputSessions.length} | Restored: ${doneUsernames.size} | Pending: ${pendingQueue.length} | Concurrency: ${concurrency}`);

Actor.on('migrating', async () => {
    await Actor.setValue('STATE', { doneUsernames: [...doneUsernames] });
    log.info(`[MIGRATION] Saved — ${doneUsernames.size} done`);
});

// ─── Session pool ─────────────────────────────────────────────────────────────

class SessionPool {
    constructor(sessions) {
        this.pool = sessions.map((s, i) => ({
            id:               i,
            sessionId:        s.sessionId,
            csrfToken:        s.csrfToken || '',
            requests:         0,
            consecutiveFails: 0,
            cooldownUntil:    0,
            dead:             false,   // permanently dead (expired session)
        }));
    }

    acquire() {
        const now       = Date.now();
        const available = this.pool
            .filter(s => !s.dead && s.cooldownUntil <= now && s.consecutiveFails < 5)
            .sort((a, b) => a.requests - b.requests);
        return available[0] ?? null;
    }

    msUntilNextAvailable() {
        const now     = Date.now();
        const soonest = this.pool
            .filter(s => !s.dead && s.cooldownUntil > now && s.consecutiveFails < 5)
            .sort((a, b) => a.cooldownUntil - b.cooldownUntil)[0];
        return soonest ? Math.max(0, soonest.cooldownUntil - now) : null;
    }

    onSuccess(session) {
        session.requests++;
        session.consecutiveFails = Math.max(0, session.consecutiveFails - 1);
    }

    // Temporary block (429) — cooldown and retry later
    onRateLimit(session) {
        session.consecutiveFails++;
        const cooldownMs      = Math.min(60_000 * session.consecutiveFails, 600_000);
        session.cooldownUntil = Date.now() + cooldownMs;
        log.warning(`Session #${session.id} rate limited (429) — cooldown ${cooldownMs / 1000}s`);
    }

    // Permanent block (401/403) — session is expired/invalid, mark dead
    onAuthFail(session, status) {
        session.dead = true;
        log.error(`Session #${session.id} auth failed (${status}) — SESSION EXPIRED. Get fresh cookies for account #${session.id}.`);
    }

    // Redirect (3xx) — session not recognised, treat as temporary
    onRedirect(session, status) {
        session.consecutiveFails++;
        const cooldownMs      = Math.min(30_000 * session.consecutiveFails, 300_000);
        session.cooldownUntil = Date.now() + cooldownMs;
        log.warning(`Session #${session.id} got redirect (${status}) — cooldown ${cooldownMs / 1000}s`);
    }

    onError(session) {
        session.consecutiveFails++;
    }

    allDead() {
        return this.pool.every(s => s.dead || s.consecutiveFails >= 5);
    }

    stats() {
        return this.pool.map(s => {
            if (s.dead) return `#${s.id}[DEAD]`;
            const cd = Math.max(0, s.cooldownUntil - Date.now());
            return `#${s.id}[${s.requests}req,${cd > 0 ? `cd${Math.ceil(cd / 1000)}s` : 'ok'}]`;
        }).join(' ');
    }
}

const sessionPool = new SessionPool(inputSessions);

// ─── Request helper ───────────────────────────────────────────────────────────
// Tries two endpoints:
//   Primary  : i.instagram.com  (mobile API — used by official Apify actor)
//   Fallback : www.instagram.com (web API)

const IG_UA = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1';

const ENDPOINTS = [
    (u) => `https://i.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(u)}`,
    (u) => `https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(u)}`,
];

async function igRequest(url, session, proxyUrl) {
    const response = await gotScraping({
        url,
        method:          'GET',
        headers: {
            'X-IG-App-ID':      '936619743392459',
            'X-ASBD-ID':        '129477',
            'X-IG-WWW-Claim':   '0',
            'Accept':           '*/*',
            'Accept-Language':  'en-US,en;q=0.9',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent':       IG_UA,
            'Referer':          'https://www.instagram.com/',
            'Cookie':  `sessionid=${session.sessionId}${session.csrfToken ? `; csrftoken=${session.csrfToken}` : ''}`,
            ...(session.csrfToken ? { 'X-CSRFToken': session.csrfToken } : {}),
        },
        proxyUrl,
        responseType:    'json',
        timeout:         { request: 15_000 },
        throwHttpErrors: false,
        retry:           { limit: 0 },
        followRedirect:  false,
    });
    return { status: response.statusCode, body: response.body };
}

async function fetchFollowers(username) {
    for (let attempt = 0; attempt < inputSessions.length * 2 + 2; attempt++) {
        // Wait for available session
        let session = sessionPool.acquire();
        while (!session) {
            if (sessionPool.allDead()) {
                return { found: false, followers: null, source: 'all_sessions_dead' };
            }
            const wait = sessionPool.msUntilNextAvailable() ?? 30_000;
            log.warning(`[${username}] All sessions cooling — waiting ${Math.ceil(wait / 1000)}s`);
            await new Promise(r => setTimeout(r, wait + 1_000));
            session = sessionPool.acquire();
        }

        const proxyUrl = await proxyConfig.newUrl(`s${session.id}`);

        // Try primary endpoint (i.instagram.com), then fallback (www.instagram.com)
        for (const endpointFn of ENDPOINTS) {
            const url = endpointFn(username);

            try {
                const { status, body } = await igRequest(url, session, proxyUrl);

                // ── Success ───────────────────────────────────────────────────
                if (status === 200) {
                    const user = body?.data?.user;
                    if (!user) {
                        sessionPool.onSuccess(session);
                        return { found: false, followers: null, source: 'no_user' };
                    }
                    sessionPool.onSuccess(session);
                    const endpoint = url.includes('i.instagram') ? 'i.ig' : 'www.ig';
                    return {
                        found:     true,
                        followers: user.edge_followed_by?.count ?? user.follower_count ?? null,
                        source:    `s${session.id}@${endpoint}`,
                    };
                }

                // ── Not found ─────────────────────────────────────────────────
                if (status === 404) {
                    sessionPool.onSuccess(session);
                    return { found: false, followers: null, source: 'not_found' };
                }

                // ── Rate limited ──────────────────────────────────────────────
                if (status === 429) {
                    sessionPool.onRateLimit(session);
                    break; // try next session, not next endpoint
                }

                // ── Auth failed — session expired ─────────────────────────────
                if (status === 401 || status === 403) {
                    sessionPool.onAuthFail(session, status);
                    break; // session is dead, try next one
                }

                // ── Redirect — session not recognised ─────────────────────────
                if (status >= 300 && status < 400) {
                    sessionPool.onRedirect(session, status);
                    break; // try next session
                }

                // ── Other error — log actual status ───────────────────────────
                log.warning(`[${username}] HTTP ${status} from ${url.includes('i.instagram') ? 'i.ig' : 'www.ig'} | session #${session.id}`);
                sessionPool.onError(session);
                // try next endpoint before giving up on session

            } catch (e) {
                log.warning(`[${username}] Request error: ${e.message?.split('\n')[0]}`);
                sessionPool.onError(session);
            }
        }

        await new Promise(r => setTimeout(r, 500));
    }

    return { found: false, followers: null, source: 'all_attempts_failed' };
}

// ─── Diagnostic smoke test ────────────────────────────────────────────────────
// Tests EACH session individually so you can see exactly which ones work.

log.info('═'.repeat(55));
log.info('DIAGNOSTIC: Testing each session individually...');
log.info('═'.repeat(55));

let anySessionWorks = false;
for (const session of sessionPool.pool) {
    const proxyUrl = await proxyConfig.newUrl(`s${session.id}`);
    for (const endpointFn of ENDPOINTS) {
        const url = endpointFn('instagram');
        try {
            const { status, body } = await igRequest(url, session, proxyUrl);
            const endpoint = url.includes('i.instagram') ? 'i.instagram.com' : 'www.instagram.com';
            if (status === 200) {
                const followers = body?.data?.user?.edge_followed_by?.count;
                log.info(`  ✅ Session #${session.id} | ${endpoint} | HTTP 200 | @instagram has ${followers?.toLocaleString()} followers`);
                anySessionWorks = true;
                break;
            } else {
                log.warning(`  ❌ Session #${session.id} | ${endpoint} | HTTP ${status}`);
                if (status === 401 || status === 403) {
                    log.error(`     → SESSION EXPIRED. Get fresh cookies from Chrome for account #${session.id}`);
                } else if (status === 429) {
                    log.warning(`     → RATE LIMITED. Wait 15-30 min before running again.`);
                } else if (status >= 300 && status < 400) {
                    log.warning(`     → REDIRECT (not authenticated). Session cookie may be invalid.`);
                }
            }
        } catch (e) {
            log.warning(`  ❌ Session #${session.id} | ${url.includes('i.instagram') ? 'i.instagram.com' : 'www.instagram.com'} | Error: ${e.message?.split('\n')[0]}`);
        }
    }
}
log.info('═'.repeat(55));

if (!anySessionWorks) {
    log.error([
        'ABORT — No session could reach Instagram successfully.',
        '',
        'All sessions returned errors. Most likely causes:',
        '  1. Sessions are expired → get fresh sessionId + csrfToken from Chrome',
        '  2. Proxy IPs are blocked → wait 30-60 min before retrying',
        '',
        'How to get fresh cookies:',
        '  Open Instagram in Chrome → DevTools (F12)',
        '  → Application → Cookies → https://www.instagram.com',
        '  → Copy "sessionid" and "csrftoken" values',
    ].join('\n'));
    await Actor.exit();
}

// ─── Main loop ────────────────────────────────────────────────────────────────

const total     = pendingQueue.length;
let   succeeded = 0;
let   failed    = 0;
let   processed = 0;

log.info(`Processing ${total} usernames | concurrency: ${concurrency}`);
log.info('─'.repeat(55));

async function processOne(username, globalIndex) {
    const progress = `[${globalIndex + 1 + doneUsernames.size}/${total + doneUsernames.size}]`;
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
log.info(`DONE — Processed: ${doneUsernames.size} | Succeeded: ${succeeded} | Failed: ${failed} | ${sessionPool.stats()}`);
await Actor.exit();
