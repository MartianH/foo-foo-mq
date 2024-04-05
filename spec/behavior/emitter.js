export default (name) => {
  let handlers = {};

  function raise (ev) {
    if (handlers[ev]) {
      const args = Array.prototype.slice.call(arguments, 1);
      handlers[ev].forEach(function (handler) {
        if (handler) {
          handler.apply(undefined, args);
        }
      });
    }
  }

  function on (ev, handle) {
    if (handlers[ev]) {
      handlers[ev].push(handle);
    } else {
      handlers[ev] = [handle];
    }
    return {
      unsubscribe: function (h) {
        handlers[ev].splice(handlers[ev].indexOf(h || handle)); // jshint ignore:line
      }
    };
  }

  function reset () {
    handlers = {};
  }

  return {
    name: name || 'default',
    handlers,
    on,
    once: on,
    raise,
    reset
  };
};
