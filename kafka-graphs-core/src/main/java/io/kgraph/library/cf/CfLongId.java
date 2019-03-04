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
 * This class represents the ID of a node in a CF scenario that has an 
 * identifier of type long. 
 *
 * @author dl
 *
 */
public class CfLongId implements CfId<Long>, Comparable<CfId<Long>> {

    private byte type;
    private Long id;

    public CfLongId() {
        id = 0L;
    }

    public CfLongId(byte type, long id) {
        this.type = type;
        this.id = id;
    }

    public boolean isItem() {
        return type == 1;
    }

    public boolean isUser() {
        return type == 0;
    }

    public boolean isOutput() {
        return type == -1;
    }

    public byte getType() {
        return type;
    }

    public Long getId() {
        return id;
    }

    /**
     * To objects of this class are the same only if both the type and the id
     * are the same.
     */
    @Override
    public int compareTo(CfId<Long> other) {
        if (type<other.getType()) {
            return -1;
        } else if (type>other.getType()){
            return 1;
        } else if (id==null && other.getId()!=null) {
            return 1;
        } else {
            return id.compareTo(other.getId());
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + type;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CfLongId other = (CfLongId) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (type != other.type) {
            return false;
        }
        return true;
    }

    /**
     * Returns a string of the format:
     * <id><\space><type>
     */
    @Override
    public String toString() {
        return id + " " + type;
    }
}
