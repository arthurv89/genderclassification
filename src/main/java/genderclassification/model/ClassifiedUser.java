package genderclassification.model;

import genderclassification.domain.User;


public class ClassifiedUser {
	private final User user;
	private final GenderProbabilities genderProbabilities;

	public ClassifiedUser(final User user, final GenderProbabilities genderProbabilities) {
		this.user = user;
		this.genderProbabilities = genderProbabilities;
	}

	public User getUser() {
		return user;
	}

	public GenderProbabilities getGenderProbabilities() {
		return genderProbabilities;
	}
	
	@Override
	public String toString() {
		return "ClassifiedUser:" + user.toString();
	}
}
