import http from 'k6/http';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';

const BASE_URL = __ENV.BASE_URL || 'http://your-remote-server:8080';
const ENDPOINT = '/api/v1/calculate-mvalue';

const testData = new SharedArray('test_data', function () {
    const f = open('./test_data.json');
    return JSON.parse(f).features;
});

export const options = {
    scenarios: {
        load: {
            executor: 'ramping-vus',
            startVUs: 5,
            stages: [
                { duration: '2m', target: 50 },
                { duration: '2m', target: 50 },
                { duration: '1m', target: 100 },
            ],
        },
    },
    thresholds: {
        http_req_duration: ['p(95)<500', 'p(99)<1000'],
        http_req_failed: ['rate<0.01'],
    },
};

export default function () {
    const feature = testData[Math.floor(Math.random() * testData.length)];
    const linkid = feature.properties.LINKID;

    const payload = JSON.stringify({
        type: 'FeatureCollection',
        features: [feature]
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(`${BASE_URL}${ENDPOINT}?col_route_id=LINKID`, payload, params);

    check(res, {
        'status is 200 or 202': (r) => r.status === 200 || r.status === 202,
        'response has body': (r) => r.body && r.body.length > 0,
    });

    sleep(0.5 + Math.random() * 0.5);
}
