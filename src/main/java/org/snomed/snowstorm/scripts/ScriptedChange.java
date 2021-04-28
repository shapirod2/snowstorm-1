package org.snomed.snowstorm.scripts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snomed.snowstorm.config.Config;
import org.snomed.snowstorm.core.data.domain.Concept;
import org.snomed.snowstorm.core.data.services.ConceptService;
import org.snomed.snowstorm.core.data.services.ServiceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.domain.PageRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ScriptedChange extends Config implements ApplicationRunner {

	@Autowired
	private ApplicationContext applicationContext;

	@Autowired
	private ConceptService conceptService;

	private static final Logger logger = LoggerFactory.getLogger(ScriptedChange.class);

	@Override
	public void run(ApplicationArguments args) throws Exception {

		inactivateConcepts(
				"MAIN/MRCMMAINT1/MRCMMAINT1-19",
				new Concept("732944001")
						.setInactivationIndicator("OUTDATED")
						.setAssociationTargets(Map.of("REPLACED_BY", Collections.singleton("1142135004"))),
				new Concept("732946004")
						.setInactivationIndicator("OUTDATED")
						.setAssociationTargets(Map.of("REPLACED_BY", Collections.singleton("1142136003"))),
				new Concept("733723002")
						.setInactivationIndicator("OUTDATED")
						.setAssociationTargets(Map.of("REPLACED_BY", Collections.singleton("1142137007"))),
				new Concept("733724008")
						.setInactivationIndicator("OUTDATED")
						.setAssociationTargets(Map.of("REPLACED_BY", Collections.singleton("1142138002"))),
				new Concept("766952006")
						.setInactivationIndicator("OUTDATED")
						.setAssociationTargets(Map.of("REPLACED_BY", Collections.singleton("1142139005"))),
				new Concept("766953001")
						.setInactivationIndicator("OUTDATED")
						.setAssociationTargets(Map.of("REPLACED_BY", Collections.singleton("1142140007"))),
				new Concept("766954007")
						.setInactivationIndicator("OUTDATED")
						.setAssociationTargets(Map.of("REPLACED_BY", Collections.singleton("1142141006"))),
				new Concept("774161007")
						.setInactivationIndicator("OUTDATED")
						.setAssociationTargets(Map.of("REPLACED_BY", Collections.singleton("1142142004"))),
				new Concept("784276002")
						.setInactivationIndicator("OUTDATED")
						.setAssociationTargets(Map.of("REPLACED_BY", Collections.singleton("1142143009"))),
				new Concept("320081000221109")
						.setInactivationIndicator("OUTDATED")
						.setAssociationTargets(Map.of("REPLACED_BY", Collections.singleton("1148793005")))
		);

		gracefullShutdown();
	}

	private void inactivateConcepts(String branch, Concept... conceptArray) throws ServiceException {
		final Map<Long, Concept> conceptInactivationInfos = Arrays.stream(conceptArray).collect(Collectors.toMap(Concept::getConceptIdAsLong, Function.identity()));
		final List<Concept> concepts = conceptService.find(conceptInactivationInfos.keySet(), null, branch, PageRequest.of(0, conceptInactivationInfos.size())).getContent();
		for (Concept concept : concepts) {
			concept.setActive(false);
			final Concept conceptInactivationInfo = conceptInactivationInfos.get(concept.getConceptIdAsLong());
			concept.setInactivationIndicator(conceptInactivationInfo.getInactivationIndicator());
			concept.setAssociationTargets(conceptInactivationInfo.getAssociationTargets());
		}
		conceptService.createUpdate(concepts, branch);
	}

	private void gracefullShutdown() throws InterruptedException {
		((ConfigurableApplicationContext)applicationContext).close();
		Thread.sleep(5_000);// Allow graceful shutdown
		System.exit(0);
	}

	public static void main(String[] args) {
		if (args.length == 0) {
			System.err.println("Please provide 1 or 2 arguments: elasticsearchHost indexPrefix?");
			System.exit(1);
		}
		String elasticsearchClusterHost = args[0];
		if (elasticsearchClusterHost.endsWith("/")) {
			elasticsearchClusterHost = elasticsearchClusterHost.substring(0, elasticsearchClusterHost.length() - 1);
		}
		String indexPrefix = "";
		if (args.length >= 2) {
			indexPrefix = args[1];
		}
		logger.info("Using index prefix {}", indexPrefix);
		logger.info("Using host {}", elasticsearchClusterHost);
		SpringApplication.run(ScriptedChange.class, "--elasticsearch.urls=" + elasticsearchClusterHost, "--elasticsearch.index.prefix=" + indexPrefix);
		System.exit(0);
	}

}
