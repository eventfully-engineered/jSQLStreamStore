# jSQLStreamStore

[![Build Status](https://travis-ci.org/seancarroll/jSQLStreamStore.svg?branch=master)](https://travis-ci.org/seancarroll/jSQLStreamStore)
[![codecov](https://codecov.io/gh/seancarroll/jSQLStreamStore/branch/master/graph/badge.svg)](https://codecov.io/gh/seancarroll/jSQLStreamStore)

Java port of Damian Hickey's [SQLStreamStore](https://github.com/SQLStreamStore/SQLStreamStore).
A Java stream store library specifically targeting SQL based implementations. 


TODO: review axon frameworks jdbc eventstore and see how they tackle avoiding gaps in subscriptions.  
I think there was something with the name of GapAware
https://github.com/AxonFramework/AxonFramework/blob/77ab0ef583d0007cf56f0da2ea7d98d823c61425/messaging/src/main/java/org/axonframework/eventhandling/GapAwareTrackingToken.java

vlingo symbio has GapRetryReader
https://github.com/vlingo/vlingo-symbio/blob/6c601baf28019e3077db06ca72ba3d0547ba5742/src/main/java/io/vlingo/symbio/store/gap/GapRetryReader.java
