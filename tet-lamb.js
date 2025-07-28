import { handler } from "./index.js";


handler({
  version: '2.0',
  routeKey: '$default',
  rawPath: '/',
  rawQueryString: '',
  headers: {
    'x-amzn-tls-cipher-suite': 'TLS_AES_128_GCM_SHA256',
    'content-length': '92',
    'x-amzn-tls-version': 'TLSv1.3',
    'x-amzn-trace-id': 'Root=1-68496109-04f218767ff65d1b4f51afa1',
    'x-forwarded-proto': 'https',
    host: '4jo6tr4lqsgqzslikbstq2aw240fizcl.lambda-url.ap-south-1.on.aws',
    'x-forwarded-port': '443',
    'content-type': 'application/json',
    'x-forwarded-for': '20.70.128.64',
    'x-key': 'qwertyuiopasdfzxc'
  },
  requestContext: {
    accountId: 'anonymous',
    apiId: '4jo6tr4lqsgqzslikbstq2aw240fizcl',
    domainName: '4jo6tr4lqsgqzslikbstq2aw240fizcl.lambda-url.ap-south-1.on.aws',
    domainPrefix: '4jo6tr4lqsgqzslikbstq2aw240fizcl',
    http: {
      method: 'POST',
      path: '/',
      protocol: 'HTTP/1.1',
      sourceIp: '20.70.128.64',
      userAgent: null
    },
    requestId: '06175c8d-d299-414e-8c55-9786a57c9b24',
    routeKey: '$default',
    stage: '$default',
    time: '11/Jun/2025:10:57:14 +0000',
    timeEpoch: 1749639434001
  },
  body: '{"device_id":"358899059930571", "latitude":"26.9169", "longitude":"75.7999", "speed":"0.00"}',
  isBase64Encoded: false
});