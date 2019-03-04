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

import org.jblas.FloatMatrix;

/**
 * Messages send in most of the CF algorithm typically must carry the id of the
 * message sender as well as the payload of the message, that is, the latent
 * vector.
 * @author dl
 *
 */
public class FloatMatrixMessage {
    CfLongId senderId;
    FloatMatrix factors;
    float score;

    public FloatMatrixMessage() {
    }

    public FloatMatrixMessage(FloatMatrixMessage msg) {
        this.senderId = msg.senderId;
        this.factors = msg.factors;
        this.score = msg.score;
    }

    public FloatMatrixMessage(
        CfLongId senderId, FloatMatrix factors,
        float score
    ) {
        this.senderId = senderId;
        this.factors = factors;
        this.score = score;
    }

    public CfLongId getSenderId() {
        return senderId;
    }

    public void setSenderId(CfLongId senderId) {
        this.senderId = senderId;
    }

    public FloatMatrix getFactors() {
        return factors;
    }

    public void setFactors(FloatMatrix factors) {
        this.factors = factors;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    @Override
    public boolean equals(Object matrix) {
        if (matrix == null) {
            return false;
        }
        FloatMatrixMessage other = (FloatMatrixMessage) matrix;
        if (senderId == null && other.senderId != null) {
            return false;
        } else if (!senderId.equals(other.senderId)) {
            return false;
        }
        if (factors == null && other.factors != null) {
            return false;
        } else if (!factors.equals(other.factors)) {
            return false;
        }
        if (score != other.score) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "[" + senderId + "] " + score + " " + factors;
    }
}
