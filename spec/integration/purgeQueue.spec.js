import '../setup.js';
import { Broker } from '../../src/index.js';
import config from './configuration.js';

/*
  Tests that queues are purged according to expected behavior:
   - auto-delete queues to NOT unsubscribed first
   - normal queues stop subscription first
   - after purge, subscription is restored
   - purging returns purged message count
   - purging does not break or disrupt channels
*/
describe('Purge Queue', function () {
  describe('when not subcribed', function () {
    const rabbit = new Broker();
    before(async function () {
      await rabbit.configure({
        connection: config.connection,
        exchanges: [
          {
            name: 'rabbot-ex.purged',
            type: 'topic',
            alternate: 'rabbot-ex.alternate',
            autoDelete: true
          }
        ],
        queues: [
          {
            name: 'rabbot-q.purged',
            autoDelete: true,
            subscribe: false,
            deadletter: 'rabbot-ex.deadletter'
          }
        ],
        bindings: [
          {
            exchange: 'rabbot-ex.purged',
            target: 'rabbot-q.purged',
            keys: 'this.is.#'
          }
        ]
      });
      await Promise.all([
        rabbit.publish('rabbot-ex.purged', { type: 'topic', routingKey: 'this.is.a.test', body: 'broadcast' }),
        rabbit.publish('rabbot-ex.purged', { type: 'topic', routingKey: 'this.is.sparta', body: 'leonidas' }),
        rabbit.publish('rabbot-ex.purged', { type: 'topic', routingKey: 'this.is.not.wine.wtf', body: 'socrates' })
      ]);
    });

    it('should have purged expected message count', function () {
      return rabbit.purgeQueue('rabbot-q.purged')
        .then(
          (purged) => {
            purged.should.equal(3);
          }
        );
    });

    it('should not re-subscribe to queue automatically (when not already subscribed)', function () {
      rabbit.getQueue('rabbot-q.purged')
        .state.should.equal('ready');
    });

    after(async function () {
      await rabbit.deleteQueue('rabbot-q.purged');
      await rabbit.close('default', true);
    });
  });

  describe('when subcribed', function () {
    describe('and queue is autodelete', function () {
      const rabbit = new Broker();
      let purgeCount;
      let harness;
      let handler;
      before(async function () {
        await rabbit.configure({
          connection: config.connection,
          exchanges: [
            {
              name: 'rabbot-ex.purged-2',
              type: 'topic',
              alternate: 'rabbot-ex.alternate',
              autoDelete: true
            }
          ],
          queues: [
            {
              name: 'rabbot-q.purged-2',
              autoDelete: true,
              subscribe: true,
              limit: 1,
              deadletter: 'rabbot-ex.deadletter'
            }
          ],
          bindings: [
            {
              exchange: 'rabbot-ex.purged-2',
              target: 'rabbot-q.purged-2',
              keys: 'this.is.#'
            }
          ]
        });
        await Promise.all([
          rabbit.publish('rabbot-ex.purged-2', { type: 'topic', routingKey: 'this.is.a.test', body: 'broadcast' }),
          rabbit.publish('rabbot-ex.purged-2', { type: 'topic', routingKey: 'this.is.sparta', body: 'leonidas' }),
          rabbit.publish('rabbot-ex.purged-2', { type: 'topic', routingKey: 'this.is.not.wine.wtf', body: 'socrates' })
        ]);
        const count = await rabbit.purgeQueue('rabbot-q.purged-3');
        purgeCount = count;
        harness = harnessFactory(rabbit, () => {}, 1);
        harness.handle('topic', (m) => {
          setTimeout(() => {
            m.ack();
          }, 100);
        });
      });

      it('should have purged some messages', function () {
        purgeCount.should.be.greaterThan(0);
        (purgeCount + harness.received.length).should.eql(3);
      });

      it('should re-subscribe to queue automatically (when not already subscribed)', function (done) {
        rabbit.getQueue('rabbot-q.purged-2')
          .state.should.equal('subscribed');
        harness.clean();
        handler = rabbit.handle('topic', (m) => {
          m.ack();
          done();
        });
        rabbit.publish('rabbot-ex.purged-2', { type: 'topic', routingKey: 'this.is.easy', body: 'stapler' });
      });

      after(async function () {
        await rabbit.deleteQueue('rabbot-q.purged-2');
        handler.remove();
        await rabbit.close('default', true);
      });
    });

    describe('and queue is not autodelete', function () {
      const rabbit = new Broker();
      let purgeCount;
      let harness;
      let handler;
      before(async function () {
        await rabbit.configure({
          connection: config.connection,
          exchanges: [
            {
              name: 'rabbot-ex.purged-3',
              type: 'topic',
              alternate: 'rabbot-ex.alternate',
              autoDelete: true
            }
          ],
          queues: [
            {
              name: 'rabbot-q.purged-3',
              autoDelete: false,
              subscribe: true,
              limit: 1,
              deadletter: 'rabbot-ex.deadletter'
            }
          ],
          bindings: [
            {
              exchange: 'rabbot-ex.purged-3',
              target: 'rabbot-q.purged-3',
              keys: 'this.is.#'
            }
          ]
        });
        await Promise.all([
          rabbit.publish('rabbot-ex.purged-3', { type: 'topic', routingKey: 'this.is.a.test', body: 'broadcast' }),
          rabbit.publish('rabbot-ex.purged-3', { type: 'topic', routingKey: 'this.is.sparta', body: 'leonidas' }),
          rabbit.publish('rabbot-ex.purged-3', { type: 'topic', routingKey: 'this.is.not.wine.wtf', body: 'socrates' })
        ]);
        const count = await rabbit.purgeQueue('rabbot-q.purged-3');
        purgeCount = count;
        harness = harnessFactory(rabbit, () => {}, 1);
        harness.handle('topic', (m) => {
          setTimeout(() => {
            m.ack();
          }, 100);
        });
      });

      it('should have purged some messages', function () {
        purgeCount.should.be.greaterThan(0);
        (purgeCount + harness.received.length).should.eql(3);
      });

      it('should re-subscribe to queue automatically (when not already subscribed)', function (done) {
        rabbit.getQueue('rabbot-q.purged-3')
          .state.should.equal('subscribed');
        harness.clean();
        handler = rabbit.handle('topic', (m) => {
          m.ack();
          done();
        });
        rabbit.publish('rabbot-ex.purged-3', { type: 'topic', routingKey: 'this.is.easy', body: 'stapler' });
      });

      after(async function () {
        await rabbit.deleteQueue('rabbot-q.purged-3');
        handler.remove();
        await rabbit.close('default', true);
      });
    });
  });
});
