const Koa = require('koa');
const app = new Koa();
var router = require('koa-router')();
const Engine = require('tingodb')();

// How many data points should be given any timespan (1 day, 7 days?)
const DATA_POINTS = Math.floor(60*24/5); // 1-day 5 minutes/tick => 288 values
// Maximum timespan to be queried (to avoid bringing in all history)
const MAX_TS_DIF = 7*24*60*60; // 7 days

const db = new Engine.Db(process.env.TINGO_DB_PATH || '../LWlogger/data', {});
const dataCollection = db.collection('livewind-data');
const lastDataCollection = db.collection('livewind-lastdata');

const isNumeric = str => !isNaN(Number.parseInt(str));
router
	.get('/', async (ctx, next) => {
		ctx.body = 'hello! You shouldn\'t be here :)';
	})
	.get('/api/query', async (ctx, next) => {
		ctx.assert(isNumeric(ctx.query.version), 400, 'Missing first parameter');
		const version = Number.parseInt(ctx.query.version);

		const result = {};
		if(ctx.query.lastData != null) { // 'all'|id:number
			const lastData = ctx.query.lastData;
			ctx.assert(isNumeric(lastData) || lastData === 'all', 400, 'wrong lastData format');
			
			result.lastData = await getLastData(lastData);
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

			result.windData = await getWindData(stationId, startTime, endTime);
		}
		ctx.body = JSON.stringify(result);
	});

app
  .use(router.routes())
  .use(router.allowedMethods());

app.listen(3000);

async function getLastData(id) {
	const query = {};
	const projection = {
		_id: 0
	};
	if(id === 'all') {
		projection.stationId = 1;
		projection.wind = 1;
		projection.gust = 1;
		projection.direction = 1;
		projection.timestamp = 1;
	}else{
		query.stationId = Number.parseInt(id);
	}

	return await dbQuery(lastDataCollection, query, projection);
}

const mean_keys = ["wind", "timestamp"];
const max_keys = ["gust"];
const add_keys = [];
const polar_keys = [{
    k: "direction",
    p: 360
}];
async function getWindData(stationId, startTime, endTime) {
	const data = await dbQuery(dataCollection, {
		stationId,
		timestamp: {
			$gte: startTime,
			$lte: endTime
		}
	}, {_id: 0, wind: 1, gust: 1, direction: 1, timestamp: 1}, {timestamp: 1});

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

function dbQuery(collection, query, projection, sort) {
	return new Promise((resolve, reject) => {
		collection.find(query, projection, (err, cursor) => {
			if(err) {
				reject(err);
			}else {
				if(sort)
					cursor = cursor.sort(sort);
				cursor.toArray((err, result) => {
					if(err) {
						reject(err);
					}else {
						resolve(result);
					}
				});
			}
		});
	});
}