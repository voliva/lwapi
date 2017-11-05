const mysql2 = require('mysql2/promise');

// This file creates a connection with MySQL server and restores it if the connection falls down.
const mySQL = {
	connection: createMySQLConnection(0),
	close: async () => {
		(await mySQL.connection).close();
	}
};

async function createMySQLConnection(nErrors) {
	nErrors = nErrors || 0;
	if(nErrors >= 6) {
		// After 6 tries, stop and exit
		console.log(`Got ${nErrors} connection errors to DB in a row, stopping lwapi :(`);
		process.exit(0);
	}

	const ret = mysql2.createConnection({
		host: process.env.SQL_DB_HOST,
		user: process.env.SQL_DB_USER,
		password: process.env.SQL_DB_PSW,
		database: process.env.SQL_DB_NAME
	});

	try {
		const connection = await ret;
		connection.once('error', (err) => {
			console.log('error', err);
			mySQL.connection = createMySQLConnection();
		});
		return connection;
	}catch(ex) {
		console.log(ex);
		await sleep(5000);
		return createMySQLConnection(nErrors+1);
	}
}
function sleep(ms) {
	return new Promise(resolve => {
		setTimeout(resolve, ms);
	});
}

module.exports = mySQL;