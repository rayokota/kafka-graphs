/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kgraph.library.bipartitematching;

import io.kgraph.VertexWithValue;

/**
 * Message for bipartite matching.
 */
public class Message {

    /**
     * Id of the vertex sending this message.
     */
    private long senderVertex;

    /**
     * Type of the message.
     */
    private enum Type {
        /**
         * Match request message sent by left vertices.
         */
        MATCH_REQUEST,
        /**
         * Grant reply message sent by right and left vertices.
         */
        REQUEST_GRANTED,
        /**
         * Denial reply message sent by right vertices.
         */
        REQUEST_DENIED
    }

    /**
     * Whether this message is a match request (null), or a message that grants
     * (true) or denies (false) another one.
     */
    private Type type = Type.MATCH_REQUEST;

    /**
     * Default constructor.
     */
    public Message() {
    }

    /**
     * Constructs a match request message.
     *
     * @param vertex Sending vertex
     */
    public Message(VertexWithValue<Long, VertexValue> vertex) {
        senderVertex = vertex.id();
        type = Type.MATCH_REQUEST;
    }

    /**
     * Constructs a match granting or denying message.
     *
     * @param vertex     Sending vertex
     * @param isGranting True iff it is a granting message
     */
    public Message(
        VertexWithValue<Long, VertexValue> vertex,
        boolean isGranting
    ) {
        this(vertex);
        type = isGranting ? Type.REQUEST_GRANTED : Type.REQUEST_DENIED;
    }

    public long getSenderVertex() {
        return senderVertex;
    }

    public boolean isGranting() {
        return type.equals(Type.REQUEST_GRANTED);
    }

    @Override
    public String toString() {
        return type + " from " + senderVertex;
    }
}
