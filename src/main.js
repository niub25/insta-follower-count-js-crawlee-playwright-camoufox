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
//
// Two types of failure, handled separately:
//
//   TEMPORARY (429 rate limit):
//     → Put session in cooldown (escalating: 60s → 120s → 300s → 600s)
//     → After cooldown expires, session is fully available again
//     → consecutiveFails resets on each success
//     → A session is NEVER permanently blocked by 429s alone
//
//   PERMANENT (401/403 auth failure):
//     → Session is marked dead=true and never used again
//     → This means the Instagram cookie has expired
//
// acquire() only checks dead + cooldown — NOT consecutiveFails.
// This is the key fix: a session with 10 consecutive 429s but an expired
// cooldown IS still usable. Previously the code blocked it forever.

class SessionPool {
    constructor(sessions) {
        this.pool = sessions.map((s, i) => ({
            id:               i,
            sessionId:        s.sessionId,
            csrfToken:        s.csrfToken || '',
            requests:         0,
            consecutiveFails: 0,
            cooldownUntil:    0,
            dead:             false,   // only true on 401/403 — permanent
        }));
    }

    // Returns available session (not dead, not in cooldown), least-used first
    acquire() {
        const now       = Date.now();
        const available = this.pool
            .filter(s => !s.dead && s.cooldownUntil <= now)
            .sort((a, b) => a.requests - b.requests);
        return available[0] ?? null;
    }

    // How long until the soonest session recovers from cooldown
    msUntilNextAvailable() {
        const now     = Date.now();
        const soonest = this.pool
            .filter(s => !s.dead && s.cooldownUntil > now)
            .sort((a, b) => a.cooldownUntil - b.cooldownUntil)[0];
        return soonest ? Math.max(0, soonest.cooldownUntil - now) : null;
    }

    onSuccess(session) {
        session.requests++;
        session.consecutiveFails = 0;   // full reset on success
    }

    // 429: temporary — cool down, but always recoverable
    onRateLimit(session) {
        session.consecutiveFails++;
        // Escalating cooldown but capped at 10 minutes
        const cooldownMs = Math.min(60_000 * session.consecutiveFails, 600_000);
        session.cooldownUntil = Date.now() + cooldownMs;
        log.warning(
            `Session #${session.id} rate limited` +
            ` (${session.consecutiveFails}× consecutive)` +
            ` — cooldown ${cooldownMs / 1000}s, will retry after`
        );
    }

    // 401/403: permanent — session cookie is expired or revoked
    onAuthFail(session, status) {
        session.dead = true;
        log.error(
            `Session #${session.id} auth failed (HTTP ${status}) — COOKIE EXPIRED.\n` +
            `  Action: get fresh sessionId + csrfToken from Chrome for account #${session.id}.`
        );
    }

    onError(session) {
        session.consecutiveFails++;
    }

    // Only true when ALL sessions have permanently failed (401/403)
    // NOT triggered by 429 rate limits
    allPermanentlyDead() {
        return this.pool.every(s => s.dead);
    }

    // True when all sessions are either dead OR currently cooling down
    allUnavailable() {
        const now = Date.now();
        return this.pool.every(s => s.dead || s.cooldownUntil > now);
    }

    stats() {
        return this.pool.map(s => {
            if (s.dead) return `#${s.id}[DEAD-AUTH]`;
            const cd = Math.max(0, s.cooldownUntil - Date.now());
            return `#${s.id}[${s.requests}req,${cd > 0 ? `cd${Math.ceil(cd / 1000)}s` : 'ok'}]`;
        }).join(' ');
    }
}

const sessionPool = new SessionPool(inputSessions);

// ─── HTTP fetch ───────────────────────────────────────────────────────────────

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

// ─── Fetch orchestrator ───────────────────────────────────────────────────────
// Key behaviour:
//   - Waits for sessions to recover from 429 cooldown (does NOT give up)
//   - Only gives up when all sessions are PERMANENTLY dead (auth failures)
//   - Tries both endpoints per session before moving to next session

async function fetchFollowers(username) {
    const maxAttempts = inputSessions.length * 2 + 2;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {

        // Wait until a session is available (may wait through multiple cooldowns)
        let session = sessionPool.acquire();
        let waitCount = 0;

        while (!session) {
            if (sessionPool.allPermanentlyDead()) {
                return { found: false, followers: null, source: 'all_sessions_dead_permanent' };
            }

            const wait = sessionPool.msUntilNextAvailable() ?? 60_000;
            log.warning(`[${username}] Sessions cooling — waiting ${Math.ceil(wait / 1000)}s for recovery (attempt ${attempt + 1})`);
            await new Promise(r => setTimeout(r, wait + 1_000));

            session = sessionPool.acquire();
            waitCount++;

            // Safety: if stuck waiting for too long, give up on THIS username only
            // (not the whole run). It will be marked failed and processing continues.
            if (waitCount > 20) {
                log.warning(`[${username}] Gave up waiting after 20 wait cycles`);
                return { found: false, followers: null, source: 'wait_timeout' };
            }
        }

        const proxyUrl = await proxyConfig.newUrl(`s${session.id}`);

        for (const endpointFn of ENDPOINTS) {
            const url = endpointFn(username);

            try {
                const { status, body } = await igRequest(url, session, proxyUrl);

                if (status === 200) {
                    const user = body?.data?.user;
                    if (!user) {
                        sessionPool.onSuccess(session);
                        return { found: false, followers: null, source: 'no_user' };
                    }
                    sessionPool.onSuccess(session);
                    return {
                        found:     true,
                        followers: user.edge_followed_by?.count ?? user.follower_count ?? null,
                        source:    `s${session.id}@${url.includes('i.instagram') ? 'i.ig' : 'www.ig'}`,
                    };
                }

                if (status === 404) {
                    sessionPool.onSuccess(session);
                    return { found: false, followers: null, source: 'not_found' };
                }

                if (status === 429) {
                    sessionPool.onRateLimit(session);
                    break; // stop trying endpoints, get a different session
                }

                if (status === 401 || status === 403) {
                    sessionPool.onAuthFail(session, status);
                    break; // session is permanently dead
                }

                if (status >= 300 && status < 400) {
                    sessionPool.onRateLimit(session); // redirect = session not recognised
                    break;
                }

                log.warning(`[${username}] HTTP ${status} on session #${session.id} | ${url.includes('i.instagram') ? 'i.ig' : 'www.ig'}`);
                sessionPool.onError(session);

            } catch (e) {
                log.warning(`[${username}] Error: ${e.message?.split('\n')[0]}`);
                sessionPool.onError(session);
            }
        }

        await new Promise(r => setTimeout(r, 300));
    }

    return { found: false, followers: null, source: 'all_attempts_failed' };
}

// ─── Diagnostic smoke test ────────────────────────────────────────────────────

log.info('═'.repeat(55));
log.info('DIAGNOSTIC: Testing each session...');
log.info('═'.repeat(55));

let anySessionWorks = false;
for (const session of sessionPool.pool) {
    const proxyUrl = await proxyConfig.newUrl(`s${session.id}`);
    for (const endpointFn of ENDPOINTS) {
        const url = endpointFn('instagram');
        try {
            const { status, body } = await igRequest(url, session, proxyUrl);
            const ep = url.includes('i.instagram') ? 'i.instagram.com' : 'www.instagram.com';
            if (status === 200) {
                const followers = body?.data?.user?.edge_followed_by?.count;
                log.info(`  ✅ Session #${session.id} | ${ep} | HTTP 200 | @instagram: ${followers?.toLocaleString()} followers`);
                anySessionWorks = true;
                break;
            } else {
                log.warning(`  ❌ Session #${session.id} | ${ep} | HTTP ${status}`);
                if (status === 401 || status === 403) log.error(`     → Cookie expired. Get fresh sessionId + csrfToken.`);
                if (status === 429)                   log.warning(`     → Rate limited. Will self-recover during run.`);
                if (status >= 300 && status < 400)    log.warning(`     → Redirect. Cookie may be invalid.`);
            }
        } catch (e) {
            log.warning(`  ❌ Session #${session.id} | Error: ${e.message?.split('\n')[0]}`);
        }
    }
}
log.info('═'.repeat(55));

if (!anySessionWorks) {
    log.error('ABORT — No session working. Refresh cookies and re-run.');
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
log.info(`DONE — Processed: ${doneUsernames.size} | Succeeded: ${succeeded} | Failed: ${failed}`);
log.info(`Sessions: ${sessionPool.stats()}`);
await Actor.exit();
