package genderclassification.initialize;

import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;

public class MemPipelineAdapter extends AbstractPipelineAdapter {
	private MemPipelineAdapter(final Pipeline pipeline) {
		super(pipeline);
	}

	protected boolean acceptFile(final String filename) {
		return filename.startsWith("out") && filename.endsWith(".txt");
	}
	
	public static MemPipelineAdapter getInstance() {
		return new MemPipelineAdapter(MemPipeline.getInstance());
	}
}
