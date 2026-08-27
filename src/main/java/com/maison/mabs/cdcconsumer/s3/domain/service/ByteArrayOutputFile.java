package com.maison.mabs.cdcconsumer.s3.domain.service;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ByteArrayOutputFile implements OutputFile {

	private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

	@Override
	public PositionOutputStream create(long blockSizeHint) {
		return createOrOverwrite(blockSizeHint);
	}

	@Override
	public PositionOutputStream createOrOverwrite(long blockSizeHint) {
		return new PositionOutputStream() {
			private long position = 0;

			@Override
			public long getPos() {
				return this.position;
			}

			@Override
			@SuppressWarnings("checkstyle:RequireThis")
			public void write(int b) throws IOException {
				ByteArrayOutputFile.this.outputStream.write(b);
				this.position++;
			}

			@Override
			@SuppressWarnings("checkstyle:RequireThis")
			public void write(byte[] b, int off, int len) throws IOException {
				ByteArrayOutputFile.this.outputStream.write(b, off, len);
				this.position += len;
			}
		};
	}

	@Override
	public boolean supportsBlockSize() {
		return false;
	}

	@Override
	public long defaultBlockSize() {
		return 0;
	}

	public byte[] toByteArray() {
		return this.outputStream.toByteArray();
	}

}
