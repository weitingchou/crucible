/**
 * generic_sql_driver.js — k6 script for the Crucible Generic SQL Workload Driver.
 *
 * Requires a custom k6 binary built with:
 *   xk6 build \
 *     --with github.com/grafana/xk6-sql \
 *     --with github.com/grafana/xk6-sql-driver-mysql
 *
 * Environment variables injected by the Taurus Orchestrator:
 *   DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME  — connection details
 *   DOWNLOADED_SQL_PATH                           — path to the annotated SQL file
 *
 * SQL file format:
 *   Queries are separated by "-- @name: <QueryName>" annotations.
 *   Each named block is treated as an independent workload with its own Trend metric.
 *
 *   Example:
 *     -- @name: TopSellingProducts
 *     SELECT product_id, SUM(revenue) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
 *
 *     -- @name: DailyActiveUsers
 *     SELECT DATE(created_at), COUNT(DISTINCT user_id) FROM events GROUP BY 1;
 */

import sql from 'k6/x/sql';
import { check } from 'k6';
import { Trend } from 'k6/metrics';

// ── Connection setup (runs once per VU initialisation) ───────────────────────

const DB_DSN = `${__ENV.DB_USER}:${__ENV.DB_PASS}@tcp(${__ENV.DB_HOST}:${__ENV.DB_PORT || '3306'})/${__ENV.DB_NAME}`;
const db = sql.open('mysql', DB_DSN);

// ── Query parsing (runs once at script load time) ─────────────────────────────

const rawSql = open(__ENV.DOWNLOADED_SQL_PATH);
const queries = parseAnnotatedSql(rawSql);

if (queries.length === 0) {
  throw new Error(
    `No annotated queries found in ${__ENV.DOWNLOADED_SQL_PATH}. ` +
    'Annotate each query with "-- @name: <QueryName>".'
  );
}

// One Trend metric per named query; labelled sql_duration_<QueryName>.
const trends = {};
queries.forEach(({ name }) => {
  trends[name] = new Trend(`sql_duration_${name}`, /* isTime= */ true);
});

// ── VU default function ───────────────────────────────────────────────────────

export default function () {
  const query = queries[Math.floor(Math.random() * queries.length)];
  const start = Date.now();

  try {
    const rows = sql.query(db, query.text);
    trends[query.name].add(Date.now() - start);
    check(rows, { [`${query.name} returned result`]: (r) => r !== null });
  } catch (err) {
    console.error(`[${query.name}] failed: ${err}`);
  }
}

// ── Teardown ──────────────────────────────────────────────────────────────────

export function teardown() {
  db.close();
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Split the SQL file on "-- @name:" annotations and return an array of
 * { name: string, text: string } objects.
 *
 * @param {string} text - Raw contents of the SQL file.
 * @returns {{ name: string, text: string }[]}
 */
function parseAnnotatedSql(text) {
  const results = [];
  // Split on the annotation marker; the first element before any marker is discarded.
  const blocks = text.split(/--\s*@name:\s*/);

  for (let i = 1; i < blocks.length; i++) {
    const newlineIdx = blocks[i].indexOf('\n');
    if (newlineIdx === -1) continue;

    const name = blocks[i].slice(0, newlineIdx).trim();
    const body = blocks[i].slice(newlineIdx + 1).trim();

    if (name && body) {
      results.push({ name, text: body });
    }
  }

  return results;
}
