package com.example.springbatch.Job;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class ReadFileJob {
    private static final String FOLDER_PATH = "/path/to/folder";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Step readFilenameStep() {
        return stepBuilderFactory.get("readFilenameStep")
                .tasklet((contribution, chunkContext) -> {
                    File folder = new File("/path/to/folder");
                    File[] files = folder.listFiles((dir, name) -> name.endsWith(".csv"));

                    if (files == null || files.length == 0) {
                        throw new RuntimeException("No CSV files found in folder");
                    }

                    String filename = files[0].getName();
                    chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("filename", filename);
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Job job1() {
        return jobBuilderFactory.get("job1")
                .start(readFilenameStep())
                .build();
    }

    @Bean
    public FlatFileItemReader<String> reader(String fileName) {
        FlatFileItemReader<String> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(fileName));
        reader.setLineMapper(new PassThroughLineMapper());
        return reader;
    }

    @Bean
    public FlatFileItemWriter<String> writer() {
        FlatFileItemWriter<String> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("output2"));
        writer.setLineAggregator(new PassThroughLineAggregator<>());
        return writer;
    }
}
