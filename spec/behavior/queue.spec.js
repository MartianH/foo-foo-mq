require('../setup.js');
const ampqQueue = require('../../src/amqp/queue');

describe('AMQP Queue', function () {
  let amqpChannelMock, options, topology, serializers;

  beforeEach(() => {
    amqpChannelMock = {
      ack: sinon.stub().callsFake(() => Promise.resolve()),
      nack: sinon.stub().callsFake(() => Promise.resolve()),
      checkQueue: sinon.stub().callsFake(() => Promise.resolve()),
      assertQueue: sinon.stub().callsFake(() => Promise.resolve())
    };

    options = {
      uniqueName: 'one-unique-name-coming-up'
    };

    topology = {
      connection: {
        getChannel: sinon.stub().callsFake(() => Promise.resolve(amqpChannelMock))
      }
    };

    serializers = sinon.stub();
  });

  describe('when executing "define"', () => {
    describe('when options.passive is not set', () => {
      it('calls assertQueue', function () {
        return ampqQueue(options, topology, serializers)
          .then((instance) => {
            return instance.define();
          })
          .then(() => {
            amqpChannelMock.checkQueue.calledOnce.should.equal(false);
            amqpChannelMock.assertQueue.calledOnce.should.equal(true);
          });
      });
    });

    describe('when options.passive is true', function () {
      it('calls checkQueue instead of assertQueue', () => {
        options.passive = true;
        return ampqQueue(options, topology, serializers)
          .then((instance) => {
            return instance.define();
          })
          .then(() => {
            amqpChannelMock.checkQueue.calledOnce.should.equal(true);
            amqpChannelMock.assertQueue.calledOnce.should.equal(false);
          });
      });
    });

    describe('when options.type is not set', function () {
      it('sets `x-queue-type` to "classic"', () => {
        const qType = 'classic';
        options.queueLimit = 1000;
        options.maxPriority = 100;
        return ampqQueue(options, topology, serializers)
          .then((instance) => {
            return instance.define();
          })
          .then(() => {
            amqpChannelMock.assertQueue.calledWith(
              options.uniqueName,
              { ...options, arguments: { 'x-queue-type': qType } });
          });
      });
    });

    describe('when options.type is "quorum"', function () {
      it('sets `x-queue-type` to "quorum" and omits incompatible fields', () => {
        options.type = 'quorum';
        options.queueLimit = 1000;
        options.maxPriority = 100;
        return ampqQueue(options, topology, serializers)
          .then((instance) => {
            return instance.define();
          })
          .then(() => {
            amqpChannelMock.assertQueue.calledWith(
              options.uniqueName,
              {
                queueLimit: options.queueLimit,
                arguments: { 'x-queue-type': options.type }
              });
          });
      });

      it('sets `x-dead-letter-strategy` argument if given', () => {
        options.type = 'quorum';
        options.queueLimit = 1000;
        options.maxPriority = 100;
        options.deadLetterStrategy = 100;
        return ampqQueue(options, topology, serializers)
          .then((instance) => {
            return instance.define();
          })
          .then(() => {
            amqpChannelMock.assertQueue.calledWith(
              options.uniqueName,
              {
                queueLimit: options.queueLimit,
                arguments: {
                  'x-queue-type': options.type,
                  'x-dead-letter-strategy': options.deadLetterStrategy
                }
              });
          });
      });
    });
  });
});
