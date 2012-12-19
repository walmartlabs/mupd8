package com.walmartlabs.mupd8.application.binary;

import java.nio.charset.Charset;

import org.json.simple.JSONObject;

public class IdentityPerformers {

    public static final Updater IDENTITY_UPDATER_INSTANCE = new Updater() {

        private final Charset UTF8 = Charset.forName("UTF-8");

        @Override
        public String getName() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Slate toSlate(byte[] bytes) {
            return new IdentitySlate(bytes);
        }

        @Override
        public void update(PerformerUtilities submitter, String stream,
                byte[] key, byte[] event, Slate slate) {
            // TODO Auto-generated method stub

        }

        @Override
        public Slate getDefaultSlate() {
            JSONObject jsonObject = new JSONObject();
            return new IdentitySlate(jsonObject.toJSONString().getBytes(UTF8));
        }

    };

    private static class IdentitySlate implements Slate {

        public final byte[] bytes;

        public IdentitySlate(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public byte[] toBytes() {
            return bytes;
        }

        @Override
        public int getBytesSize() {
            return bytes.length;
        }

    }
}
