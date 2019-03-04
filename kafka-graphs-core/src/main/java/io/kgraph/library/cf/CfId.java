/**
 * Copyright 2014 Grafos.ml
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kgraph.library.cf;

/**
 * Classes that implement this interface represent the ID of a node in a 
 * user-item graph. Nodes in this case typically represent either users or items 
 * and are identified by a number or a string (e.g. an item description). To 
 * avoid conflicts between user and item ids, this interface defines the type 
 * of the node as well, not just the identifier. The type is a byte value and 
 * can by set and interpreted in an application-specific manner.
 *
 * @author dl
 *
 * @param <T>
 */
public interface CfId<T> {
    /**
     * Returns the type of the node.
     * @return
     */
    byte getType();

    /**
     * Returns the identifier of the node.
     * @return
     */
    T getId();
}
