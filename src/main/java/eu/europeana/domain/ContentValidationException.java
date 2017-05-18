package eu.europeana.domain;

/**
 * This exception is thrown when verifying if object content is downloaded properly fails.
 * Created by Patrick Ehlert on 18-5-17.
 */
public class ContentValidationException extends Exception {

    private static final long serialVersionUID = -9051104227935832718L;

    public ContentValidationException(String errorMsg) {
        super(errorMsg);
    }

    public ContentValidationException(String errorMsg, Throwable t) {
        super(errorMsg, t);
    }

    public ContentValidationException(Throwable t) {
        super(t);
    }

}
