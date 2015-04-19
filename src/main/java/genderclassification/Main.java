package genderclassification;

import java.io.IOException;

import genderclassification.classify.ClassifyJob;
import genderclassification.createmodel.ModelJob;

public class Main {
	public static void main(String[] args) throws IOException {
		while(true) {
			ModelJob.runJob();
			ClassifyJob.runJob();
		}
	}
}
