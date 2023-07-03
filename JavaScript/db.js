'use strict';

const { Pool } = require('pg');

const where = (conditions) => {
  const operators = ['>=', '<=', '<>', '>', '<'];
  const clauses = [];
  const args = [];
  let i = 1;

  for (const key in conditions) {
    let value = conditions[key];
    let condition = `${key} = $${i}`;

    if (typeof value === 'string') {
      for (const op of operators) {
        if (value.startsWith(op)) {
          condition = `${key} ${op} $${i}`;
          value = value.substring(op.length);
        } else if (value.includes('*') || value.includes('?')) {
          value = value.replace(/\*/g, '%').replace(/\?/g, '_');
          condition = `${key} LIKE $${i}`;
        }
      }
    }

    i++;
    args.push(value);
    clauses.push(condition);
  }

  const clause = clauses.join(' AND ');
  return { clause, args };
};

const MODE_ROWS = 0;
const MODE_VALUE = 1;
const MODE_ROW = 2;
const MODE_COL = 3;
const MODE_COUNT = 4;

class Cursor {
  constructor(database, table) {
    this.database = database;
    this.table = table;
    this.cols = null;
    this.rows = null;
    this.rowCount = 0;
    this.ready = false;
    this.mode = MODE_ROWS;
    this.whereClause = undefined;
    this.columns = ['*'];
    this.args = [];
    this.orderBy = undefined;
  }

  resolve(result) {
    const { rows, fields, rowCount } = result;
    this.rows = rows;
    this.cols = fields;
    this.rowCount = rowCount;
  }

  where(conditions) {
    const { clause, args } = where(conditions);
    this.whereClause = clause;
    this.args = args;
    return this;
  }

  fields(list) {
    this.columns = list;
    return this;
  }

  value() {
    this.mode = MODE_VALUE;
    return this;
  }

  row() {
    this.mode = MODE_ROW;
    return this;
  }

  col(name) {
    this.mode = MODE_COL;
    this.columnName = name;
    return this;
  }

  count() {
    this.mode = MODE_COUNT;
    return this;
  }

  order(name, order) {
    this.orderBy = name;
    if (order) this.orderBy += ` ${order.toUpperCase()}`;
    return this;
  }

  then(callback) {
    const { mode, table, columns, args } = this;
    const { whereClause, orderBy } = this;
    const fields = columns.join(', ');
    let sql = `SELECT ${fields} FROM ${table}`;
    if (whereClause) sql += ` WHERE ${whereClause}`;
    if (orderBy) sql += ` ORDER BY ${orderBy}`;
    this.database.query(sql, args,  (err, res) => {
      if (err) return void callback(err);
      this.resolve(res);
      const result = this.processMode(mode);
      callback(null, result);
    });
    return this;
  }

  processMode(mode) {
    const { rows, cols, columnName, rowCount } = this;
    if (mode === MODE_ROW) return rows[0];
    if (mode === MODE_COUNT) return rowCount;
    if (mode === MODE_COL) return rows.map((row) => row[columnName]);
    if (mode === MODE_VALUE) return rows[0][cols[0].name];
    return rows;
  }
}

class Database {
  constructor(config, logger) {
    this.pool = new Pool(config);
    this.config = config;
    this.logger = logger || console.log;
  }

  query(sql, values, callback) {
    const { logger } = this;
    if (typeof values === 'function') {
      callback = values;
      values = [];
    }
    const startTime = new Date().getTime();
    logger({ sql, values });
    this.pool.query(sql, values, (err, res) => {
      const endTime = new Date().getTime();
      const executionTime = endTime - startTime;
      logger(`Execution time: ${executionTime}`);
      if (callback) callback(err, res);
    });
  }

  select(table) {
    return new Cursor(this, table);
  }

  close() {
    this.pool.end();
  }
}

module.exports = {
  open: (config, logger) => new Database(config, logger),
};
