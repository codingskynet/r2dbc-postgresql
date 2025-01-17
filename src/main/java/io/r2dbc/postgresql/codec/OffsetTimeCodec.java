/*
 * Copyright 2020 the original author or authors.
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
import io.r2dbc.postgresql.util.ByteBufUtils;

import java.time.OffsetTime;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMETZ;

final class OffsetTimeCodec extends AbstractTemporalCodec<OffsetTime> {

    private final ByteBufAllocator byteBufAllocator;

    OffsetTimeCodec(ByteBufAllocator byteBufAllocator) {
        super(OffsetTime.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    PostgresqlObjectId getDefaultType() {
        return TIMETZ;
    }

    @Override
    OffsetTime doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends OffsetTime> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return decodeTemporal(buffer, dataType, format, OffsetTime.class, OffsetTime::from);
    }

    @Override
    Parameter doEncode(OffsetTime value) {
        Assert.requireNonNull(value, "value must not be null");

        return create(TIMETZ, FORMAT_TEXT, () -> ByteBufUtils.encode(this.byteBufAllocator, value.toString()));
    }

    @Override
    public Parameter encodeNull() {
        return createNull(TIMETZ, FORMAT_TEXT);
    }

}
