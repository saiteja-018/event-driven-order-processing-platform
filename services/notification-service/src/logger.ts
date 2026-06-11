export const logger = {
  info(obj:any){ console.log(JSON.stringify({ timestamp: new Date().toISOString(), level: 'INFO', service: 'notification-service', ...obj })); },
  warn(obj:any){ console.log(JSON.stringify({ timestamp: new Date().toISOString(), level: 'WARN', service: 'notification-service', ...obj })); },
  error(obj:any){ console.error(JSON.stringify({ timestamp: new Date().toISOString(), level: 'ERROR', service: 'notification-service', ...obj })); }
};
