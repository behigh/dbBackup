const config = {
	host: 'localhost', // Database host
	port: 3306, // Database port
	user: 'root', // Database user
	password: '', // Database password
	database: '', // Database name
	tables: [], // List of tables to backup
	ignoreTables: [], // List of tables to ignore
	target_path: './backups', // Backup path, should by exists
	fileName: '[database].sql',
	compress: 'lz4c', // Can lz4c or bzip2
}

module.exports = config
