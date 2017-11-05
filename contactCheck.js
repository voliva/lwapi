const mysql = require('./mysqlConnection');

async function main() {
    try {
        const connection = await mysql.connection;
        const result = await connection.query("SELECT * FROM livewind.sponsorRequests WHERE status <> 'seen'");
        return result[0] && result[0].length;
    }catch(ex) {
        console.error(ex);
        return true;
    }
}

main().then((result) => {
    if(result) {
        console.log('there are new messages');
    }
    return mysql.close();
});