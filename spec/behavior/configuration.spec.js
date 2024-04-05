import '../setup.js';

import { Broker as BaseBroker } from '../../src/index.js';

describe('Configuration', function () {
  const noOp = function () {};
  const connection = {
    name: 'test',
    configureBindings: noOp,
    configureExchanges: noOp,
    configureQueues: noOp,
    once: noOp
  };
  class Broker extends BaseBroker {
    constructor (conn) {
      super();
      this.connection = conn;
      this.configurations = {};
      this.configuring = {};
    }

    addConnection () {
      return Promise.resolve(this.connection);
    }

    emit () { }
  }

  describe('with valid configuration', function () {
    const config = {
      exchanges: [{}],
      queues: [{}],
      bindings: [{}]
    };
    let connectionMock;
    before(function () {
      connectionMock = sinon.mock(connection);
      connectionMock.expects('configureExchanges')
        .once()
        .withArgs(config.exchanges)
        .returns(Promise.resolve(true));
      connectionMock.expects('configureQueues')
        .once()
        .withArgs(config.queues)
        .returns(Promise.resolve(true));
      connectionMock.expects('configureBindings')
        .once()
        .withArgs(config.bindings, 'test')
        .returns(Promise.resolve(true));

      const broker = new Broker(connection);

      return broker.configure(config);
    });

    it('should make expected calls', function () {
      connectionMock.verify();
    });

    after(function () {
      connectionMock.restore();
    });
  });

  describe('with an initially failed connection', function () {
    const config = {
      exchanges: [{}],
      queues: [{}],
      bindings: [{}]
    };
    let connectionMock;
    before(function () {
      connectionMock = sinon.mock(connection);
      connectionMock.expects('configureExchanges')
        .once()
        .withArgs(config.exchanges)
        .returns(Promise.resolve(true));
      connectionMock.expects('configureQueues')
        .once()
        .withArgs(config.queues)
        .returns(Promise.resolve(true));
      connectionMock.expects('configureBindings')
        .once()
        .withArgs(config.bindings, 'test')
        .returns(Promise.resolve(true));

      const broker = new Broker(connection);

      return broker.configure(config);
    });

    it('should make expected calls', function () {
      connectionMock.verify();
    });

    after(function () {
      connectionMock.restore();
    });
  });

  describe('when exchange creation fails', function () {
    const config = {
      exchanges: [{}],
      queues: [{}],
      bindings: [{}]
    };
    let connectionMock;
    let error;
    before(function () {
      connectionMock = sinon.mock(connection);
      connectionMock.expects('configureExchanges')
        .once()
        .withArgs(config.exchanges)
        .returns(Promise.reject(new Error("Not feelin' it today")));
      connectionMock.expects('configureQueues')
        .never();
      connectionMock.expects('configureBindings')
        .never();

      const broker = new Broker(connection);

      return broker.configure(config)
        .then(null, function (err) {
          error = err;
        });
    });

    it('should make expected calls', function () {
      connectionMock.verify();
    });

    it('should return error', function () {
      error.toString().should.equal("Error: Not feelin' it today");
    });

    after(function () {
      connectionMock.restore();
    });
  });

  describe('when queue creation fails', function () {
    const config = {
      exchanges: [{}],
      queues: [{}],
      bindings: [{}]
    };
    let connectionMock;
    let error;
    before(function () {
      connectionMock = sinon.mock(connection);
      connectionMock.expects('configureExchanges')
        .once()
        .withArgs(config.exchanges)
        .returns(Promise.resolve(true));
      connectionMock.expects('configureQueues')
        .once()
        .withArgs(config.queues)
        .returns(Promise.reject(new Error("Not feelin' it today")));
      connectionMock.expects('configureBindings')
        .never();

      const broker = new Broker(connection);

      return broker.configure(config)
        .then(null, function (err) {
          error = err;
        });
    });

    it('should make expected calls', function () {
      connectionMock.verify();
    });

    it('should return error', function () {
      error.toString().should.equal("Error: Not feelin' it today");
    });

    after(function () {
      connectionMock.restore();
    });
  });

  describe('when binding creation fails', function () {
    const config = {
      exchanges: [{}],
      queues: [{}],
      bindings: [{}]
    };
    let connectionMock;
    let error;
    before(function () {
      connectionMock = sinon.mock(connection);
      connectionMock.expects('configureExchanges')
        .once()
        .withArgs(config.exchanges)
        .returns(Promise.resolve(true));
      connectionMock.expects('configureQueues')
        .once()
        .withArgs(config.queues)
        .returns(Promise.resolve(true));
      connectionMock.expects('configureBindings')
        .once()
        .withArgs(config.bindings, 'test')
        .returns(Promise.reject(new Error("Not feelin' it today")));

      const broker = new Broker(connection);

      return broker.configure(config)
        .then(null, function (err) {
          error = err;
        });
    });

    it('should make expected calls', function () {
      connectionMock.verify();
    });

    it('should return error', function () {
      error.toString().should.equal("Error: Not feelin' it today");
    });

    after(function () {
      connectionMock.restore();
    });
  });
});
