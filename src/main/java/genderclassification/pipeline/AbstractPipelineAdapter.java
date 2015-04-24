package genderclassification.pipeline;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;

import com.google.common.base.Function;
import com.google.common.base.Throwables;

public abstract class AbstractPipelineAdapter {
    private final Pipeline pipeline;

    public AbstractPipelineAdapter(final Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public List<String> parseResult(final File outputFolder) throws IOException {
        final Path path = FileSystems.getDefault().getPath(outputFolder.getAbsolutePath());
        return Files.list(path).filter(p -> acceptFile(p.getFileName().getFileName().toString())).map(p -> {
            try {
                return FileUtils.readLines(p.toFile());
            } catch (final Exception e) {
                throw Throwables.propagate(e);
            }
        }).reduce(new ArrayList<String>(), (t, u) -> {
            final List<String> list = new ArrayList<String>();
            list.addAll(t);
            list.addAll(u);
            return list;
        });
    }

    public <K, V> File performPipeline(final Function<Pipeline, PTable<K, V>> execute, File outputFolder)
            throws IOException {
        final File outputFile = new File(outputFolder, UUID.randomUUID().toString());

        pipeline.enableDebug();

        final PTable<K, V> result = execute.apply(pipeline);
        pipeline.writeTextFile(result, outputFile.getAbsolutePath());
        pipeline.done();

        return outputFile;
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    protected abstract boolean acceptFile(final String filename);
}
