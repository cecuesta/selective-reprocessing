package edu.doc_ti.jfcp.selec_reproc.interceptor;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.doc_ti.jfcp.selec_reproc.interceptor.model.CDRRecord;
import edu.doc_ti.jfcp.selec_reproc.interceptor.utils.Constants;
import edu.doc_ti.jfcp.selec_reproc.interceptor.utils.FileRegister;
import edu.doc_ti.jfcp.selec_reproc.interceptor.utils.JdbcInserter;

public class CDRInterceptor implements Interceptor {

	private static final Logger LOG = LoggerFactory.getLogger(CDRInterceptor.class);
	private FileRegister fileR;
	
	private final String delimiter;
	private JdbcInserter jdbcInt;
	private String jdbcUrl;

	
	public CDRInterceptor(String delimiter, String jdbcUrl) {
		this.delimiter = delimiter;
		this.jdbcUrl = jdbcUrl ;
	}

	@Override
	public void close() {		
		LOG.info("## Closing CDRInterceptor");
		fileR = null;
		jdbcInt.insert(fileR);
		jdbcInt.close();
	}

	@Override
	public void initialize() {
		LOG.info("## Initialize CDRInterceptor");
		fileR = new FileRegister("", 0);
		jdbcInt = new JdbcInserter(jdbcUrl);
		
	}

	@Override
	public Event intercept(Event event) {
		if (event != null && event.getBody() != null && event.getHeaders() != null){
			
			String file = fileR.getFilename(event.getHeaders().get(Constants.FILE));
			if (file != null){
				
				if (file.startsWith(Constants.RELOAD_PREFIX)) {
					String[] fileParts = file.split(Constants.RELOAD_PREFIX);
					if (fileParts.length == 2 && !fileParts[1].isEmpty()){
						file = fileParts[1];
					}
				}
				
				CDRRecord cdr = new CDRRecord (event.getBody(), event.getHeaders());

				if (!fileR.getFile().equalsIgnoreCase(file)){
					jdbcInt.insert(fileR);
					LOG.info("###### Change of file, data: " + fileR.getFile() + " size: " + fileR.getPosition() );

					fileR.setFile(file);
					fileR.setPosition(1);
				} else {
					int updatePos = fileR.getPosition() + 1;
					fileR.setPosition(updatePos);
				}
				
				cdr.modifyBody(String.valueOf(fileR.getPosition()),delimiter);
				cdr.modifyBody(fileR.getFile(),delimiter);
				
				event.setBody(cdr.getBody());
				event.setHeaders(cdr.getHeaders());
//				LOG.info("New CDR: headers=[{}] body=[{}]", cdr.getHeaders().toString(), new String(cdr.getBody()));
				cdr = null;
			} else {
				LOG.warn("Unknown CDR file [{}]. Invalid Event", event.getHeaders().get(Constants.FILE));
				event = null;
			}
		}
				
		return event;
		
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> interceptedEvents = new ArrayList<Event>(events.size());
		
        for (Event event : events){
        	Event interceptedEvent = intercept(event);
        	if(interceptedEvent!= null){
        		interceptedEvents.add(interceptedEvent);
        	}
        }

        return interceptedEvents;	
    }

	public static class Builder implements Interceptor.Builder
    {
		private String delimiter;
		private String jdbcUrl;

		@Override
        public void configure(Context context) {
			// Flume configuration
			delimiter = context.getString(Constants.DELIMITER, Constants.DELIMITER_DEFAULT);
			jdbcUrl = context.getString(Constants.JDBC_URL);
        }

        @Override
        public Interceptor build() {
        	LOG.info(String.format("Creating CDRInterceptor: delimiter=[%s], url=[%s]",  delimiter, jdbcUrl ));
        	return new CDRInterceptor(delimiter, jdbcUrl);
        }
    }
}
