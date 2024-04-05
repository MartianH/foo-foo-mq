import log from 'bole';
import debug from 'debug';

const debugEnv = process.env.DEBUG;

const debugOut = {
  write: function (data) {
    const entry = JSON.parse(data);
    debug(entry.name)(entry.level, entry.message);
  }
};

if (debugEnv) {
  log.output({
    level: 'debug',
    stream: debugOut
  });
}
export function logger (config) {
  if (typeof config === 'string') {
    return log(config);
  } else {
    log.output(config);
    return log;
  }
}
