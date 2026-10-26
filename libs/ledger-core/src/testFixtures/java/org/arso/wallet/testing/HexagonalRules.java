package org.arso.wallet.testing;

import static com.tngtech.archunit.base.DescribedPredicate.alwaysTrue;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.onionArchitecture;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import java.util.TreeSet;

/** Hexagonal / onion rules from spec 4, shared by every module. */
public final class HexagonalRules {

    private static final String EVENTS = "org.arso.wallet.events";

    private HexagonalRules() {}

    public static void check(String basePackage) {
        var classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages(basePackage);

        onion(basePackage, classes).check(classes);

        classes().that().resideInAPackage(basePackage + ".domain..")
                .should().onlyDependOnClassesThat().resideInAnyPackage("java..", "org.arso.wallet..domain..")
                .allowEmptyShould(true)
                .check(classes);

        classes().that().resideInAPackage(basePackage + ".port..")
                .should().onlyDependOnClassesThat()
                .resideInAnyPackage("java..", "org.arso.wallet..domain..", "org.arso.wallet..port..", EVENTS)
                .allowEmptyShould(true)
                .check(classes);

        classes().that().resideInAPackage(basePackage + ".application..")
                .should().onlyDependOnClassesThat()
                .resideInAnyPackage(
                        "java..",
                        "org.arso.wallet..domain..",
                        "org.arso.wallet..port..",
                        "org.arso.wallet..application..",
                        EVENTS)
                .allowEmptyShould(true)
                .check(classes);

        slices().matching(basePackage + ".(**)").should().beFreeOfCycles().allowEmptyShould(true).check(classes);

        noClasses().should().dependOnClassesThat()
                .haveNameMatching("software\\.amazon\\.awssdk\\.services\\.dynamodb\\..*Scan(Request|Response|Iterable).*")
                .because("a Scan reads the whole table")
                .check(classes);
    }

    private static com.tngtech.archunit.library.Architectures.OnionArchitecture onion(String base, JavaClasses classes) {
        var adapters = new TreeSet<String>();
        var prefix = base + ".adapter.";
        classes.stream()
                .map(JavaClass::getPackageName)
                .filter(p -> p.startsWith(prefix))
                .forEach(p -> adapters.add(p.substring(prefix.length()).split("\\.")[0]));
        var onion = onionArchitecture()
                .domainModels(base + ".domain..")
                .domainServices(base + ".port..")
                .applicationServices(base + ".application..")
                .withOptionalLayers(true)
                .ignoreDependency(resideInAPackage(base + ".config.."), alwaysTrue());
        adapters.forEach(a -> onion.adapter(a, prefix + a + ".."));
        return onion;
    }
}
