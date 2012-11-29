//
//  AMQPConsumerThread.m
//  Objective-C wrapper for librabbitmq-c
//
//  Copyright 2009 Max Wolter. All rights reserved.
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.
//

#import "AMQPConsumerThread.h"
#import "AMQPWrapper.h"

#import "amqp.h"
#import "amqp_framing.h"
#import <string.h>
#import <stdlib.h>

#import "AMQPConsumer.h"
#import "AMQPChannel.h"
#import "AMQPQueue.h"
#import "AMQPMessage.h"

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
@implementation AMQPConsumerThread
{
    NSDictionary        *_configuration;
    NSString            *_exchangeKey;
    NSString            *_topic;
    
    AMQPConnection      *_connection;
    AMQPChannel         *_channel;
    AMQPExchange        *_exchange;
    AMQPQueue           *_queue;
    AMQPConsumer        *_consumer;
    
    dispatch_queue_t    _callbackQueue;
	AMQPConsumer        *consumer;
    
    
	NSObject<AMQPConsumerThreadDelegate> *delegate;
}

@synthesize delegate;

#pragma mark - Dealloc and Initialization

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
- (void)dealloc
{
    [self _tearDown];
    dispatch_release(_callbackQueue);
    
	[super dealloc];
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
- (id)initWithConfiguration:(NSDictionary *)configuration exchangeKey:(NSString *)exchangeKey topic:(NSString *)topic delegate:(id)theDelegate callbackQueue:(dispatch_queue_t)callbackQueue
{
    if((self = [super init])) {
        _configuration  = [configuration retain];
        _exchangeKey    = [exchangeKey copy];
        _topic          = [topic copy];
        
        if(!callbackQueue) {
            callbackQueue = dispatch_get_main_queue();
        }
        dispatch_retain(callbackQueue);
        _callbackQueue = callbackQueue;
        delegate = theDelegate;
    }
    return self;
}

#pragma mark - NSThread

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
- (void)main
{
    CTXLogVerbose(CTXLogContextMessageBroker, @"<starting: consumer_thread: (%p) topic: %@>", self, _topic);
    [self _setup];
    CTXLogVerbose(CTXLogContextMessageBroker, @"<started: consumer_thread: (%p) topic: %@>", self, _topic);
    
	while(![self isCancelled]) {
        @autoreleasepool {
            AMQPMessage *message = [self _consume];
            if(message) {
                CTXLogVerbose(CTXLogContextMessageBroker, @"<consumer_thread: (%p) topic: %@ received message>", self, _topic);
                dispatch_async(_callbackQueue, ^{
                    [delegate amqpConsumerThreadReceivedNewMessage:message];
                });
            }
        }
	}

    CTXLogVerbose(CTXLogContextMessageBroker, @"<stopping: consumer_thread: (%p) topic: %@>", self, _topic);
    [self _tearDown];
    CTXLogVerbose(CTXLogContextMessageBroker, @"<stopped: consumer_thread: (%p) topic: %@>", self, _topic);
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
- (void)_setup
{
    [self _connect];
    [self _setupExchange];
    [self _setupConsumerQueue];
    [self _setupConsumer];
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
- (void)_tearDown
{
    [_consumer release], _consumer = nil;
    [_queue release], _queue = nil;
    [_exchange release], _exchange = nil;
    [_channel close];
    [_channel release], _channel = nil;
    [_connection disconnect];
    [_connection release], _connection = nil;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
- (void)_connect
{
    NSString *host      = [_configuration objectForKey:@"host"];
    int port            = [[_configuration objectForKey:@"port"] intValue];
    NSString *username  = [_configuration objectForKey:@"username"];
    NSString *password  = [_configuration objectForKey:@"password"];
    NSString *vhost     = [_configuration objectForKey:@"vhost"];
    
    _connection = [[AMQPConnection alloc] init];
//    @try {
        CTXLogVerbose(CTXLogContextMessageBroker, @"==> Connecting to host <%@:%d>...", host, port);
        [_connection connectToHost:host onPort:port];
        CTXLogVerbose(CTXLogContextMessageBroker, @"==> Connected!");
        
        CTXLogVerbose(CTXLogContextMessageBroker, @"==> Authenticating user <%@>...", username);
        [_connection loginAsUser:username withPasswort:password onVHost:vhost];
        CTXLogVerbose(CTXLogContextMessageBroker, @"==> Success!");
    
    _channel = [[_connection openChannel] retain];
//    }
//    @catch(NSException *exception) {
//        if(outError != NULL) {
//            NSInteger errorCode = -1010;
//            NSDictionary *userInfo = (@{
//                                      NSLocalizedDescriptionKey         : exception.name,
//                                      NSLocalizedFailureReasonErrorKey  : exception.reason});
//            NSError *error = [NSError errorWithDomain:@"com.ef.smart.classroom.broker.amqp" code:errorCode userInfo:userInfo];
//            *outError = error;
//        }
//        return NO;
//    }
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
- (void)_setupExchange
{
    _exchange = [[AMQPExchange alloc] initTopicExchangeWithName:@"com.ef" onChannel:_channel isPassive:NO isDurable:NO getsAutoDeleted:YES];
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
- (void)_setupConsumerQueue
{
    _queue = [[AMQPQueue alloc] initWithName:@"" onChannel:_channel isPassive:NO isExclusive:NO isDurable:NO getsAutoDeleted:YES];
    [_queue bindToExchange:_exchange withKey:_topic];
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
- (void)_setupConsumer
{
    _consumer = [_queue startConsumerWithAcknowledgements:NO isExclusive:NO receiveLocalMessages:NO];
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
- (AMQPMessage *)_consume
{
	amqp_frame_t frame;
	int result = -1;
	size_t receivedBytes = 0;
	size_t bodySize = -1;
	amqp_bytes_t body;
	amqp_basic_deliver_t *delivery;
	amqp_basic_properties_t *properties;
	
	AMQPMessage *message = nil;
    AMQPChannel *channel = _channel;
	
	amqp_maybe_release_buffers(channel.connection.internalConnection);
	
	while(!message && ![self isCancelled]) {
        if (!amqp_frames_enqueued(channel.connection.internalConnection) &&
            !amqp_data_in_buffer(channel.connection.internalConnection)) {
            if (1) {
                int sock = amqp_get_sockfd(channel.connection.internalConnection);
                //                printf("socket: %d\n", sock);
                
                /* Watch socket fd to see when it has input. */
                fd_set read_flags;
                int ret = 0;
                do {
                    FD_ZERO(&read_flags);
                    FD_SET(sock, &read_flags);
                    
                    struct timeval timeout;
                    
                    /* Wait upto a second. */
                    timeout.tv_sec = 1;
                    timeout.tv_usec = 0;
                    
                    ret = select(sock+1, &read_flags, NULL, NULL, &timeout);
                    if (ret == -1) {
//                        printf("select: %s\n", strerror(errno));
                    }
                    else if (ret == 0) {
//                        printf("select timedout\n");
                    }
                    if (FD_ISSET(sock, &read_flags)) {
//                        printf("Flag is set\n");
                    }
                } while (ret == 0 && ![self isCancelled]);
            }
        }
        
        
		// a complete message delivery consists of at least three frames:
		
		// Frame #1: method frame with method basic.deliver
		result = amqp_simple_wait_frame(channel.connection.internalConnection, &frame);
		if(result < 0) {
//            NSLog(@"result = %d", result);
            return nil;
        }
		
		if(frame.frame_type != AMQP_FRAME_METHOD || frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) {
            continue;
        }
		
		delivery = (amqp_basic_deliver_t*)frame.payload.method.decoded;
		
		// Frame #2: header frame containing body size
		result = amqp_simple_wait_frame(channel.connection.internalConnection, &frame);
		if(result < 0) {
            NSLog(@"result = %d", result);
            return nil;
        }
		
		if(frame.frame_type != AMQP_FRAME_HEADER)
		{
            NSLog(@"frame.frame_type != AMQP_FRAME_HEADER");
			return nil;
		}
		
		properties = (amqp_basic_properties_t*)frame.payload.properties.decoded;
		
		bodySize = frame.payload.properties.body_size;
		receivedBytes = 0;
		body = amqp_bytes_malloc(bodySize);
		
		// Frame #3+: body frames
		while(receivedBytes < bodySize)
		{
			result = amqp_simple_wait_frame(channel.connection.internalConnection, &frame);
			if(result < 0) {
                NSLog(@"result = %d", result);
                return nil;
            }
			
			if(frame.frame_type != AMQP_FRAME_BODY)
			{
                NSLog(@"frame.frame_type != AMQP_FRAME_BODY");
                
				return nil;
			}
			
			receivedBytes += frame.payload.body_fragment.len;
			memcpy(body.bytes, frame.payload.body_fragment.bytes, frame.payload.body_fragment.len);
		}
        
		message = [AMQPMessage messageFromBody:body withDeliveryProperties:delivery withMessageProperties:properties receivedAt:[NSDate date]];
		
		amqp_bytes_free(body);
	}
	
	return message;
}
@end
