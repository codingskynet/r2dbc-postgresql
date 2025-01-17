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
import io.r2dbc.postgresql.client.Parameter;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT8;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT8_ARRAY;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link LongArrayCodec}.
 */
final class LongArrayCodecUnitTests {

    private final ByteBuf BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1)
        .writeInt(0)
        .writeInt(20)
        .writeInt(2)
        .writeInt(2)
        .writeInt(8)
        .writeLong(100L)
        .writeInt(8)
        .writeLong(200L);

    @Test
    void decodeItem() {
        assertThat(new LongArrayCodec(TEST).decode(BINARY_ARRAY, 0, FORMAT_BINARY, Long[].class)).isEqualTo(new Long[]{100L, 200L});
        assertThat(new LongArrayCodec(TEST).decode(TEST.buffer(), 0, FORMAT_BINARY, Long[].class)).isEqualTo(new Long[]{});
        assertThat(new LongArrayCodec(TEST).decode(encode(TEST, "{100,200}"), 0, FORMAT_TEXT, Long[].class)).isEqualTo(new Long[]{100L, 200L});
        assertThat(new LongArrayCodec(TEST).decode(encode(TEST, "{}"), 0, FORMAT_TEXT, Long[].class)).isEqualTo(new Long[]{});
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void decodeObject() {
        assertThat(((Codec) new LongArrayCodec(TEST)).decode(BINARY_ARRAY, 0, FORMAT_BINARY, Object.class)).isEqualTo(new Long[]{100L, 200L});
        assertThat(((Codec) new LongArrayCodec(TEST)).decode(encode(TEST, "{100,200}"), 0, FORMAT_TEXT, Object.class)).isEqualTo(new Long[]{100L, 200L});
    }

    @Test
    void doCanDecode() {
        assertThat(new LongArrayCodec(TEST).doCanDecode(INT8, FORMAT_TEXT)).isFalse();
        assertThat(new LongArrayCodec(TEST).doCanDecode(INT8_ARRAY, FORMAT_TEXT)).isTrue();
        assertThat(new LongArrayCodec(TEST).doCanDecode(INT8_ARRAY, FORMAT_BINARY)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LongArrayCodec(TEST).doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeArray() {
        assertThat(new LongArrayCodec(TEST).encodeArray(() -> encode(TEST, "{100,200}")))
            .hasFormat(FORMAT_TEXT)
            .hasType(INT8_ARRAY.getObjectId())
            .hasValue(encode(TEST, "{100,200}"));
    }

    @Test
    void encodeItem() {
        assertThat(new LongArrayCodec(TEST).doEncodeText(100L)).isEqualTo("100");
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LongArrayCodec(TEST).doEncodeText(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new LongArrayCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, INT8_ARRAY.getObjectId(), NULL_VALUE));
    }

}
