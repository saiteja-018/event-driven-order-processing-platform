export const logger = {
  info(obj:any){ console.log(JSON.stringify({ timestamp: new Date().toISOString(), level: 'INFO', service: 'order-service', ...obj })); },
  warn(obj:any){ console.log(JSON.stringify({ timestamp: new Date().toISOString(), level: 'WARN', service: 'order-service', ...obj })); },
  error(obj:any){ console.error(JSON.stringify({ timestamp: new Date().toISOString(), level: 'ERROR', service: 'order-service', ...obj })); }
};
