package com.github.minyk.morphlinesmr.shaded.com.googlecode.jcsv.writer.internal;

import com.github.minyk.morphlinesmr.shaded.com.googlecode.jcsv.CSVStrategy;
import com.github.minyk.morphlinesmr.shaded.com.googlecode.jcsv.writer.CSVColumnJoiner;
import com.github.minyk.morphlinesmr.shaded.com.googlecode.jcsv.writer.CSVEntryConverter;
import com.github.minyk.morphlinesmr.shaded.com.googlecode.jcsv.writer.CSVWriter;
import com.github.minyk.morphlinesmr.shaded.com.googlecode.jcsv.writer.internal.CSVWriterBuilder;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

public class CSVWriterImpl<E> implements CSVWriter<E> {

	private final Writer writer;
	private final CSVStrategy strategy;
	private final CSVEntryConverter<E> entryConverter;
	private final CSVColumnJoiner columnJoiner;

	CSVWriterImpl(CSVWriterBuilder<E> builder) {
		this.writer = builder.writer;
		this.strategy = builder.strategy;
		this.entryConverter = builder.entryConverter;
		this.columnJoiner = builder.columnJoiner;
	}

	@Override
	public void writeAll(List<E> data) throws IOException {
		for (E e : data) {
			write(e);
		}
	}

	@Override
	public void write(E e) throws IOException {
		StringBuilder sb = new StringBuilder();

		String[] columns = entryConverter.convertEntry(e);
		String line = columnJoiner.joinColumns(columns, strategy);

		sb.append(line);
		sb.append(System.getProperty("line.separator"));

		writer.append(sb.toString());
	}

    @Override
    public String writeString(E e) {
        StringBuilder sb = new StringBuilder();

        String[] columns = entryConverter.convertEntry(e);
        String line = columnJoiner.joinColumns(columns, strategy);

        sb.append(line);
        //sb.append(System.getProperty("line.separator"));
        return sb.toString();
    }

	@Override
	public void flush() throws IOException {
		writer.flush();
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}
}
