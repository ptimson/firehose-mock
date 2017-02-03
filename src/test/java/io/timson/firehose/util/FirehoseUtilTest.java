package io.timson.firehose.util;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class FirehoseUtilTest {

    @Test
    public void randomFreePort_shouldReturnFreeRandomPort_WhenAvailable() throws Exception {
        Integer port = FirehoseUtil.randomFreePort();
        assertThat(port, is(greaterThan(0)));
    }

    @Test
    public void isEmpty_ShouldBeTrue_WhenNull() throws Exception {
        assertThat(FirehoseUtil.isEmpty(null), is(true));
    }

    @Test
    public void isEmpty_ShouldBeTrue_WhenEmptyStringLiteral() throws Exception {
        assertThat(FirehoseUtil.isEmpty(""), is(true));
    }

    @Test
    public void isEmpty_ShouldBeFalse_WhenStringNotEmpty() throws Exception {
        assertThat(FirehoseUtil.isEmpty("notEmpty"), is(false));
    }

}