package io.timson.firehose.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.timson.firehose.util.FirehoseUtil;

public class PutResponse {

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String recordIdChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/+";
    private static final int recordIdLength = 224;

    @JsonProperty("RecordId")
    private final String recordId;

    public PutResponse() {
        recordId = FirehoseUtil.randomString(recordIdChars, recordIdLength);
    }

    public String body() throws JsonProcessingException {
        return mapper.writeValueAsString(this);
    }

    public String getRecordId() {
        return recordId;
    }

}
