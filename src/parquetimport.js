const log = require('electron-log')
const parquet = require('parquetjs-lite');
import * as path from 'path'
import db from 'sqlite'
import { finished } from 'stream';
import * as _ from 'lodash'
const isAlpha = (ch: string): boolean => /^[A-Z]$/i.test(ch)

/* generate a SQL table name from pathname */
const genTableName = (pathname: string): string => {
    const extName = path.extname(pathname)
    const baseName = path.basename(pathname, extName)
    let baseIdent = mapIdent(baseName)
    if (!isAlpha(baseIdent[0])) {
      baseIdent = 't_' + baseIdent
    }
    const tableName = uniquify(baseIdent)
    return tableName
  }

let uniqMap = {}

/* add a numeric _N suffix to an identifer to make it unique */
const uniquify = (src: string): string => {
    let entry = uniqMap[src]
    if (entry === undefined) {
      uniqMap[src] = 1
      return src  // no suffix needed
    }
    const ret = src + '_' + entry.toString()
    uniqMap[src] = ++entry
    return ret
  }

/* map to alphanumeric */
const mapIdent = (src: string): string => {
    const ret = src.replace(/[^a-z0-9_]/gi, '_')
    return ret
  }


const getColType = (logicalType) => {
    switch(logicalType) {
        case 'BYTE_ARRAY':
            return 'text'
        case 'DOUBLE':
        case 'FLOAT':
            return 'real'
        case 'INT32':
        case 'INT64':
            return 'integer'
        default:
            return 'text'
      } 
  }


const importData = async (md: FileMetadata, cursor): Promise<FileMetadata> => {
  try {
    console.log('in import data')
    const tableName = md.tableName
    const qTableName = "'" + tableName + "'"
    const dropStmt = 'drop table if exists ' + qTableName
    const idts = _.zip(md.columnIds, md.columnTypes)
    const typedCols = idts.map(([cid, ct]) => "'" + cid + "' " + (ct ? ct : '')) // eslint-disable-line
    const schemaStr = typedCols.join(', ')
    const createStmt = 'create table ' + qTableName + ' ( ' + schemaStr + ' )'
    await db.run(dropStmt)
    await db.run(createStmt)
    log.log('table created')
    const qs = Array(md.columnNames.length).fill('?')
    const insertStmtStr = 'insert into ' + qTableName + ' values (' + qs.join(', ') + ')'
    const insertStmt = await db.prepare(insertStmtStr)
    let record = null;
    while (record = await cursor.next()) {
        let rowVals =  md.columnNames.map(x => record[x])
        insertStmt.run(rowVals) 
    }
    db.run('commit')
    reader.close();
    return md
  } catch (err) {
    log.error(err, err.stack)
    throw err
  }
}


export const fastImport = async (pathname: string): Promise<FileMetadata> => {
    const importStart = process.hrtime()
    try {
        let reader = await parquet.ParquetReader.openFile('/home/chrisma/code/parquet-js-play/trades.gz.parquet');
        let cursor = reader.getCursor();
        let columnNames = cursor.schema.fieldList.map(x => x.name)
        let primitiveTypes = cursor.schema.fieldList.map(x => x.primitiveType)
        let columnTypes = primitiveTypes.map(x => getColType(x))
        let table_name = genTableName('/home/chrisma/code/parquet-js-play/trades.gz.parquet')
        const fileMetadata = {
            columnIds: columnNames,
            columnNames: columnNames,
            columnTypes: columnTypes,
            rowCount: 0,
            tableName: table_name,
            csvOptions: {}
          }
        importData(fileMetadata, cursor)
        return fileMetadata
    } catch (err) {
      log.error('caught error during parquet import: ', err, err.stack)
      throw err
    }
  }