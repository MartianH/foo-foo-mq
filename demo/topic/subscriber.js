
import subscriberTopicLeft from './subscriber-topic-left.js';
import subscriberTopicRight from './subscriber-topic-right.js';
import topology from './topology.js';
import rabbit from '../../src/index.js';

topology(rabbit)
  .then(function () {
    subscriberTopicLeft(rabbit);
    subscriberTopicRight(rabbit);
  });
