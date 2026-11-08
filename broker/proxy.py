import zmq

context = zmq.Context()

sub = context.socket(zmq.XSUB)
sub.bind("tcp://*:5557")

pub = context.socket(zmq.XPUB)
pub.bind("tcp://*:5558")

zmq.proxy(pub, sub)

pub.close()
sub.close()
context.close()
