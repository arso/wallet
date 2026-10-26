package org.arso.wallet.events;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

class ArchitectureTest {

    @Test
    void messageRecordsArePlainJava() {
        var classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("org.arso.wallet.events");

        classes().that().resideInAPackage("org.arso.wallet.events")
                .should().onlyDependOnClassesThat().resideInAnyPackage("java..", "org.arso.wallet.events")
                .check(classes);
    }
}
