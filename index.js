const path = require('path')
const fs = require('fs')
const mysql = require('mysql')
const os = require('os')

let configFile = process.argv[2]

if (!configFile) {
	console.error('Config file is required. Run node . pathToConfig.js')
	process.exit()
}

try {
	configFile = fs.realpathSync(configFile)
} catch (e) {
	console.error(e.toString())
	process.exit()
}

const cfg = require(configFile)

cfg.fileName = cfg.fileName.replace('[database]', cfg.database)

try {
	cfg.target_path = fs.realpathSync(cfg.target_path)
} catch (e) {
	console.error(e.toString())
	process.exit()
}

cfg.fileTS = new Date().getTime().toString()

const connection = mysql.createConnection({
	host: cfg.host,
	port: cfg.port,
	user: cfg.user,
	password: cfg.password,
	database: cfg.database,
	multipleStatements: true,
})

connection.connect(err => {
	if (err) {
		console.error(err)
		process.exit()
	}

	connection.on('error', function (e) {
		console.error(e)
	})

	connection.query('SHOW TABLES;', (err, results, fields) => {
		if (err) {
			console.error(err)
			process.exit()
		}
		if (results.length > 0) {
			const fieldName = Object.keys(results[0])[0]

			const arrIgnoreTables = []
			let ignoreTablesStr = ''

			results.map(entry => {
				if (cfg.tables.length && cfg.tables.indexOf(entry[fieldName]) === -1) {
					arrIgnoreTables.push(entry[fieldName])
				}

				if (cfg.ignoreTables.length && cfg.ignoreTables.indexOf(entry[fieldName]) !== -1) {
					arrIgnoreTables.push(entry[fieldName])
				}
			})

			if (arrIgnoreTables.length) {
				ignoreTablesStr = ` --ignore-table=${cfg.database}.` + arrIgnoreTables.join(` --ignore-table=${cfg.database}.`) + ``
			}

			getKeys(results, fieldName, [], 0, results.length, cfg, ignoreTablesStr, arrIgnoreTables, connection)
		}

	})
})

function test(...args) {
	console.log(...args)
	process.exit()
}

function getKeys(arrTables, fieldName, arrKeys, currIndex, count, cfg, ignoreTablesStr, arrIgnoreTables, clinet) {

	if (currIndex < count) {

		if (arrIgnoreTables.indexOf(arrTables[currIndex][fieldName]) === -1) {

			const query = `
				SELECT
					S1.INDEX_NAME AS Key_name,
				    S1.SEQ_IN_INDEX AS Seq_in_index,
					S1.COLUMN_NAME AS Column_name,
				    S1.SUB_PART AS Sub_part
				FROM (SELECT * FROM information_schema.STATISTICS S WHERE S.TABLE_SCHEMA='${cfg.database}' and S.TABLE_NAME='${arrTables[currIndex][fieldName]}') S1 
				LEFT JOIN (SELECT * FROM information_schema.KEY_COLUMN_USAGE K where K.TABLE_SCHEMA='${cfg.database}' and K.TABLE_NAME='${arrTables[currIndex][fieldName]}') K1 on S1.COLUMN_NAME = K1.COLUMN_NAME
				WHERE K1.TABLE_SCHEMA is null;
			`

			clinet.query(query, (err, results, fields) => {
				if (err) {
					console.error(err)
					return
				}
				if (results.length > 0) {
					arrKeys['`' + arrTables[currIndex][fieldName] + '`'] = {}
					for (let i = 0; i < results.length; i++) {
						if (results[i].Non_unique !== 0) {
							if (arrKeys['`' + arrTables[currIndex][fieldName] + '`']['`' + results[i].Key_name + '`'] === undefined) {
								arrKeys['`' + arrTables[currIndex][fieldName] + '`']['`' + results[i].Key_name + '`'] = []
							}
							arrKeys['`' + arrTables[currIndex][fieldName] + '`']['`' + results[i].Key_name + '`'][(results[i].Seq_in_index) - 1] = ['`' + results[i].Column_name + '`' + (results[i].Sub_part == null ? '' : '(' + results[i].Sub_part + ')')]
							//arrKeys.push({ key: '`' + results[i].Key_name + '`', tableName: '`' + arrTables[currIndex][fieldName] + '`', isNonUnique: results[i].Non_unique, column: '`' + results[i].Column_name + '`' + (results[i].Sub_part == null ? '' : '(' + results[i].Sub_part + ')') });
						}
					}
				}
				setTimeout(() => {
					getKeys(arrTables, fieldName, arrKeys, currIndex + 1, count, cfg, ignoreTablesStr, arrIgnoreTables, clinet)
				}, 10)
			})
		} else {
			setTimeout(() => {
				getKeys(arrTables, fieldName, arrKeys, currIndex + 1, count, cfg, ignoreTablesStr, arrIgnoreTables, clinet)
			}, 10)
		}
	} else {
		const dropIndexQueries = []
		const createIndexQueries = []
		const arrTables = Object.keys(arrKeys)

		for (let i = 0; i < arrTables.length; i++) {
			const arrIndexes = Object.keys(arrKeys[arrTables[i]])
			for (let j = 0; j < arrIndexes.length; j++) {
				dropIndexQueries.push(`DROP INDEX ${arrIndexes[j]} ON ${arrTables[i]};`)
				createIndexQueries.push(`ALTER TABLE ${arrTables[i]} ADD INDEX ${arrIndexes[j]} (${arrKeys[arrTables[i]][arrIndexes[j]].join(',')});`)
				//createIndexQueries.push('ALTER TABLE ' + arrTables[i] + ' ADD ' + ' INDEX ' + arrIndexes[j] + ' (' + arrKeys[arrTables[i]][arrIndexes[j]].join(',') + ') ;')
				//createIndexQueries.push('ALTER TABLE ' + arrKeys[i].tableName + ' ADD ' + (arrKeys[i].isNonUnique == 0 ? 'UNIQUE' : '') + ' INDEX ' + arrKeys[i].key + ' (' + arrKeys[i].column + ') ;');
			}
		}

		const dropIndexFile = path.join(cfg.target_path, `${cfg.database}_${cfg.fileTS}_DROP_INDEX.sql`)
		const createIndexFile = path.join(cfg.target_path, `${cfg.database}_${cfg.fileTS}_CREATE_INDEX.sql`)



		fs.writeFile(dropIndexFile, dropIndexQueries.join('\n'), err => {
			if (err) {
				console.error(err.toString())
				process.exit()
			}
		})
		fs.writeFile(createIndexFile, createIndexQueries.join('\n'), err => {
			if (err) {
				console.error(err.toString())
				process.exit()
			}
		})

		let args = [
			`-h ${cfg.host}`,
			(cfg.port ? `-P ${cfg.port }` : ''),
			`-u ${cfg.user}`,
			(cfg.password ? `-p${cfg.password }` : ''),
			ignoreTablesStr,
			cfg.database
		].filter(item => item.length).join(' ')

		let commands = []

		if (parseInt(cfg.takeProcedure) === 1) {
			commands.push(`mysqldump --comments --triggers --routines --no-data ${args}`)
		} else {
			commands.push(`mysqldump --comments --no-data --skip-triggers ${args}`)
		}

		const catCmd = process.platform === 'win32' ? 'type' : 'cat'

		commands.push(`${catCmd} ${dropIndexFile}`)
		commands.push(`mysqldump --extended-insert --disable-keys --flush-logs --no-autocommit --no-create-info ${args}`)
		commands.push(`${catCmd} ${createIndexFile}`)

		let str = process.platform === 'win32' ? '(' + commands.join(' & ') + ')' : '{ ' + commands.join(';' + os.EOL) + '; }'

		switch (cfg.compress) {
			case 'bzip2':
				str += ' | bzip2 > ' + path.join(cfg.target_path, cfg.fileName + '.bz2');
				break;
			case 'lz4c':
				str += ' | lz4c -4f  - ' + path.join(cfg.target_path, cfg.fileName + '.lz4')
				break;
			default:
				str += ' > ' + path.join(cfg.target_path, cfg.fileName)
				break
		}



		// console.log(str);
		//var sys = require('sys')
		const exec = require('child_process').exec

		function puts(error, stdout, stderr) {
			fs.unlinkSync(dropIndexFile)
			fs.unlinkSync(createIndexFile)
			error && console.log(error);
			process.exit()
			//   process.send("test");

		}

		exec(str, puts);

		//console.log(str)

		//res.send('Done...');
	}
}
