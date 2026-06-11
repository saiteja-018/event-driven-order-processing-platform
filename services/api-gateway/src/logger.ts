type LogLevel = 'INFO'|'WARN'|'ERROR'|'DEBUG';
export const logger = {
  log(level: LogLevel, obj: any) {
    const out = {
      timestamp: new Date().toISOString(),
      level,
      ...obj
    };
    // ensure valid JSON line
    console.log(JSON.stringify(out));
  },
  info(obj:any){ this.log('INFO', obj); },
  warn(obj:any){ this.log('WARN', obj); },
  error(obj:any){ this.log('ERROR', obj); },
  debug(obj:any){ this.log('DEBUG', obj); }
};
