/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;

import java.util.function.Supplier;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.FLOAT4_ARRAY;

/**
 * @since 0.8.8
 */
final class FloatArrayCodec extends AbstractArrayCodec<Float> {

    FloatArrayCodec(ByteBufAllocator byteBufAllocator) {
        super(byteBufAllocator, Float.class);
    }

    @Override
    public Parameter encodeNull() {
        return createNull(FLOAT4_ARRAY, FORMAT_TEXT);
    }

    @Override
    Float doDecodeBinary(ByteBuf byteBuffer) {
        return byteBuffer.readFloat();
    }

    @Override
    Float doDecodeText(String text) {
        return Float.parseFloat(text);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(type, "type must not be null");

        return FLOAT4_ARRAY == type;
    }

    @Override
    Parameter encodeArray(Supplier<ByteBuf> encodedSupplier) {
        return create(FLOAT4_ARRAY, FORMAT_TEXT, encodedSupplier);
    }

    @Override
    String doEncodeText(Float value) {
        Assert.requireNonNull(value, "value must not be null");

        return value.toString();
    }

}
