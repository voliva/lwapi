const Koa = require('koa');
const app = new Koa();
const router = require('koa-router')();
const cors = require('koa-cors');
const mysql = require('./mysqlConnection');
const bodyParser = require('koa-bodyparser');
const request = require('request-promise-native');

// How many data points should be given any timespan (1 day, 7 days?)
const DATA_POINTS = Math.floor(60*24/5); // 1-day 5 minutes/tick => 288 values
// Maximum timespan to be queried (to avoid bringing in all history)
const MAX_TS_DIF = 7*24*60*60; // 7 days

const isNumeric = str => !isNaN(Number.parseInt(str));
router
	.get('/', async (ctx, next) => {
		ctx.body = 'hello! You shouldn\'t be here :)';
	})
	.get('/query', async (ctx, next) => {
		ctx.assert(isNumeric(ctx.query.version), 400, 'Missing first parameter');
		const version = Number.parseInt(ctx.query.version);
		const sessionId = Number.parseInt(ctx.query.sessionId) || null;

		const result = {};
		if(ctx.query.lastData != null) { // 'all'|id:number
			const lastData = ctx.query.lastData;
			ctx.assert(isNumeric(lastData) || lastData === 'all', 400, 'wrong lastData format');
			
			result.lastData = formatToArrayTable(await getLastData(lastData));
			saveUserEvent(sessionId, 'lastData', lastData);
		}
		if(ctx.query.windData != null) { // id:number;start:number;end:number
			const windData = ctx.query.windData.split(';');
			ctx.assert(
				windData.length === 3 &&
				windData.every(d => isNumeric(d))
			, 400, 'wrong windData format');

			const parsedWindData = windData.map(d => Number.parseInt(d));
			const stationId = parsedWindData[0];
			const startTime = parsedWindData[1];
			const endTime = Math.max(startTime, parsedWindData[2]);
			const timeDiff = endTime - startTime;
			ctx.assert(timeDiff < MAX_TS_DIF, 400, 'windData dataset too big');

			result.windData = formatToArrayTable(await getWindData(stationId, startTime, endTime));
			saveUserEvent(sessionId, 'windData', windData[0], windData[1], windData[2]);
		}
		ctx.body = JSON.stringify(result);
	})
	.get('/stats', async (ctx, next) => {
		const connection = await mysql.connection;
		// Last week data
		const today = new Date();
		const weekAgo = new Date(today.getTime() - 7*24*60*60*1000);
		// sessionId, timestamp, query, target, paramTimeStart, paramTimeEnd
		const data = (await connection.query('SELECT * FROM userStats WHERE timestamp > ' + Math.floor(weekAgo.getTime()/1000)))[0];
		const users = data.reduce((users, row) => {
			users[row.sessionId] = users[row.sessionId] || [];
			users[row.sessionId].push(row);
			return users;
		}, {});
		const stationDataQueries = data.filter(d => !!d.target);
		const stations = stationDataQueries.reduce((stations, row) => {
			stations[row.target] = stations[row.target] || [];
			stations[row.target].push(row);
			return stations;
		}, {});

		const usersArr = [];
		for(id in users) {
			usersArr.push(users[id]);
		}
		usersArr.sort((u1, u2) => u2.length - u1.length);

		const stationsArr = [];
		for(id in stations) {
			stationsArr.push({
				id,
				rows: stations[id]
			});
		}
		stationsArr.sort((s1, s2) => s2.rows.length - s1.rows.length);

		const result = {};
		result.nActiveUsers = usersArr.length;
		result.totalEvents = data.length;
		result.nEventsByUser = usersArr.map(u => u.length);
		result.eventsByStation = stationsArr.map(s => ({
			id: s.id,
			events: s.rows.length
		}));

		ctx.body = result;
	})
	.post('/contact', async (ctx, next) => {
		ctx.assert(!!ctx.query.subject, 400, 'Missing subject');
		const subject = ctx.query.subject;
		const body = ctx.request.body;

		let googleResult = false;
		try {
			googleResult = await validateCaptcha(body.gRecaptchaResponse);
		}catch(ex) {}

		if(googleResult) {
			const connection = await mysql.connection;
		
			connection.execute(`INSERT INTO
				sponsorRequests (company, email, web, comments)
				VALUES (?,?,?,?)
			`, [
				body.company,
				body.email,
				body.web,
				body.comments
			]);

			ctx.body = 'ok';
		}else {
			ctx.body = 'err';
		}
	});

app
  .use(cors())
  .use(bodyParser())
  .use(router.routes())
  .use(router.allowedMethods());

app.listen(8000);

function formatToArrayTable(rows) {
	if(rows.length === 0) return [];
	const keys = Object.keys(rows[0]);
	
	return [
		keys,
		...rows.map(r => keys.map(k => {
			if(typeof r[k] === 'number') {
				return Math.round(r[k] * 100) / 100;
			}else {
				return r[k];
			}
		}))
	]
}

async function validateCaptcha(value) {
	const postData = {
		secret: process.env.GOOGLE_CAPTCHA_SECRET,
		response: value
	};

	return request.post('https://www.google.com/recaptcha/api/siteverify', {
		form: postData
	}).then(r => JSON.parse(r).success);
}

async function saveUserEvent(sessionId, query, target, timeStart = null, timeEnd = null) {
	const connection = await mysql.connection;
	const timestamp = Math.floor(new Date().getTime() / 1000);

	connection.execute(`INSERT INTO
		userStats (sessionId, timestamp, query, target, paramTimeStart, paramTimeEnd)
		VALUES (?,?,?,?,?,?)
	`, [
		sessionId,
		timestamp,
		query,
		target,
		timeStart,
		timeEnd
	]);
}
async function getLastData(id) {
	const connection = await mysql.connection;

	let rows;
	if(id === 'all') {
		rows = (await connection.query(`
			SELECT stationId,wind,gust,direction,timestamp
			FROM lastWeatherData`))[0];
	}else{
		rows = (await connection.execute(`
		SELECT stationId, timestamp, temperature, humidity, pressure, wind, gust, direction, rain
		FROM lastWeatherData
		WHERE stationId = ?`, [id]))[0];
	}

	return rows;
}

const mean_keys = ["wind", "timestamp"];
const max_keys = ["gust"];
const add_keys = [];
const polar_keys = [{
    k: "direction",
    p: 360
}];
async function getWindData(stationId, startTime, endTime) {
	const connection = await mysql.connection;

	const data = (await connection.execute(`
		SELECT wind,gust,direction,timestamp
		FROM weatherData
		WHERE stationId = ?
		AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp ASC`, [Number.parseInt(stationId), startTime, endTime]))[0];
	if(data.length < DATA_POINTS) {
		return data;
	}

	const ret = [];
	const step = data.length / DATA_POINTS;
	for(let i=0; i<data.length; i += step) {
		const iStart = Math.ceil(i);
		const iEnd = 1 + Math.min( // Adding 1 because .slice makes the end non-inclusive
			data.length - 1,
			Math.floor(i+step) - (Number.isInteger(i+step) ? 1 : 0)
		);
		const dataToReduce = data.slice(iStart, iEnd);
		if(dataToReduce.length === 0) continue; // should never happen but... anyway

		const row = {
		};
		max_keys.forEach(k => {
			const max = dataToReduce.reduce((max, d) => {
				if (d[k] == null) return max;
				return Math.max(max, d[k]);
			}, -Number.MAX_SAFE_INTEGER);
			row[k] = max === -Number.MAX_SAFE_INTEGER ? null : max;
		});
		add_keys.forEach(k => {
			row[k] = dataToReduce.reduce((sum, d) => {
				if (d[k] == null) return sum;
				return sum + d;
			}, 0);
		});
		mean_keys.forEach(k => {
			const meanSum = dataToReduce.reduce((meanSum, d) => {
				if (d[k] == null) return meanSum;
				return {
					sum: meanSum.sum + d[k],
					values: meanSum.values + 1
				};
			}, {
				sum: 0,
				values: 0
			});

			row[k] = meanSum.values > 0 ? meanSum.sum / meanSum.values : null;
		});
		polar_keys.forEach(({k, p}) => {
			let distances = 0;
			let currentSum = [0, 0];

			const polarSum = dataToReduce.reduce((polarSum, d) => {
				if (d[k] == null) return polarSum;
				return {
					sum: [
						polarSum.sum[0] + Math.sin(2 * Math.PI * d[k] / p),
						polarSum.sum[1] + Math.cos(2 * Math.PI * d[k] / p),
					],
					values: polarSum.values + 1
				};
			}, {
				sum: [0,0],
				values: 0
			});

			if(polarSum.values > 0) {
				let rad = Math.atan2(
					polarSum.sum[0] / polarSum.values,
					polarSum.sum[1] / polarSum.values);
				if (rad < 0) rad += 2 * Math.PI;
				row[k] = p * rad / (2 * Math.PI);
			} else {
				row[k] = null;
			}
		});
		ret.push(row);
	}

	return ret;
}
