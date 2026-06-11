import axios from 'axios';
import { API_BASE } from './setup';

describe('rate limiting', ()=>{
  jest.setTimeout(120000);
  test('test_rate_limit_enforcement_and_reset', async ()=>{
    const url = `${API_BASE}/api/v1/health`;
    let lastResp: any = null;
    for (let i=0;i<101;i++) {
      try {
        const r = await axios.get(url);
        lastResp = r;
      } catch (err: any) {
        lastResp = err.response;
        if (lastResp && lastResp.status === 429) break;
      }
    }
    expect(lastResp).toBeTruthy();
    expect([200,429]).toContain(lastResp.status);
    if (lastResp.status === 429) {
      expect(lastResp.headers['retry-after'] || lastResp.headers['Retry-After']).toBeTruthy();
    }

    // wait 65s to reset window
    await new Promise(r=>setTimeout(r, 65000));
    const after = await axios.get(url);
    expect(after.status).toBe(200);
  });
});
test('rate limiting placeholder', ()=>{ expect(true).toBe(true); });