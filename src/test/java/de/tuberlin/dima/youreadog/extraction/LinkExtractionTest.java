package de.tuberlin.dima.youreadog.extraction;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

public class LinkExtractionTest {
	
	private LinkExtractor extractor;
	
	@Before
	public void initializeExtractor () {
		extractor = new LinkExtractor();
	}
	
	@Test
	public void testHrefExtraction() {
		
		String html = "<a name=\"header.logo\" href=\"http://www.zalando.de/index.html\"><img height=\"53\" width=\"190\" src=\"http://skin.ztat.net/s/04g/img/logos/shops/schuhe.jpg\" alt=\"Schuhe und Mode bei Zalando.de\" title=\"Schuhe und Mode bei Zalando.de\"/></a>";
		Set<String> links = extractor.extractLinks("zalando.de", html);
		assertEquals(1, links.size());
		assertEquals("www.zalando.de", new LinkedList<String>(links).get(0));
		
	}
	
	@Test
	public void testEncodedHrefExtraction() {
		
		String html = "<a name=\"header.logo\" href=\"http&#58;&#47;&#47;g.live.com&#47;9uxp9en-us&#47;\"><img height=\"53\" width=\"190\" src=\"http://skin.ztat.net/s/04g/img/logos/shops/schuhe.jpg\" alt=\"Schuhe und Mode bei Zalando.de\" title=\"Schuhe und Mode bei Zalando.de\"/></a>";
		Set<String> links = extractor.extractLinks("zalando.de", html);
		assertEquals(1, links.size());
		assertEquals("g.live.com", new LinkedList<String>(links).get(0));
		
	}
	
	@Test
	public void testSeveralExtractions () {
		
		String html = "<a name=\"header.logo\" href=\"http&#58;&#47;&#47;g.live.com&#47;9uxp9en-us&#47;\"><img height=\"53\" width=\"190\" src=\"http://skin.ztat.net/s/04g/img/logos/shops/schuhe.jpg\" alt=\"Schuhe und Mode bei Zalando.de\" title=\"Schuhe und Mode bei Zalando.de\"/></a>";
		html += "<a name=\"header.logo\" href=\"http://www.zalando.de/index.html\"><img height=\"53\" width=\"190\" src=\"http://skin.ztat.net/s/04g/img/logos/shops/schuhe.jpg\" alt=\"Schuhe und Mode bei Zalando.de\" title=\"Schuhe und Mode bei Zalando.de\"/></a>";
		html += "test<a href=\"https:/www.test.de\">test</a>";
		Set<String> links = extractor.extractLinks("zalando.de", html);
		assertEquals(3, links.size());
		
	}

}
