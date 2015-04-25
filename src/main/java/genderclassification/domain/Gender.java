package genderclassification.domain;

public enum Gender {
    M(0), F(1), U(2);

    private final int position;

    private Gender(final int position) {
        this.position = position;
    }
    
    public int getPosition() {
		return position;
	}
}