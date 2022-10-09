const MEASURE_WITH_AGG = 'CSR Sec nonCTP CVRdown'
const MEASURE = 'CSR Sec nonCTP Curvature Kb High'

describe('Side bar: measures', () => {
    beforeEach(() => {
        cy.visit('http://localhost:4173');
    })

    it('create new measures', () => {
        cy.get(':nth-child(1)').contains(MEASURE).click();
        cy.get(':nth-child(3)').get('.MuiAccordionSummary-content').contains('Measures').parent().parent()
            .get('.MuiAccordionDetails-root div[data-virtuoso-scroller="true"]').children().contains(MEASURE).should('be.visible');
    })

    it('measures selected expands on field click', () => {
        cy.get('div[data-test-id="virtuoso-scroller"]').contains(MEASURE).click();
        cy.get(':nth-child(3)').get('.MuiAccordionSummary-content').contains('Measures').parent().parent()
            .get('.MuiAccordionDetails-root div[data-virtuoso-scroller="true"]').children().contains(MEASURE).should('be.visible');
    })

    it('should remove selected measure', () => {
        cy.get('div[data-test-id="virtuoso-scroller"]').contains(MEASURE).click();
        cy.get(':nth-child(3)').get('.MuiAccordionSummary-content').contains('Measures').parent().parent()
            .get('.MuiAccordionDetails-root div[data-virtuoso-scroller="true"]').children().contains(MEASURE)
            .parentsUntil('.MuiBox-root')
            .siblings('button')
            .click()

        cy.get(':nth-child(3)').get('.MuiAccordionSummary-content').contains('Measures').parent().parent()
            .get('.MuiAccordionDetails-root div[data-virtuoso-scroller="true"] div[data-test-id="virtuoso-item-list"] div')
            .should('not.exist');

    })

    it('should remove selected measure with agg selector', () => {
        cy.get('div[data-test-id="virtuoso-scroller"]').contains(MEASURE_WITH_AGG).click();
        cy.get(':nth-child(3)').get('.MuiAccordionSummary-content').contains('Measures').parent().parent()
            .get('.MuiAccordionDetails-root div[data-virtuoso-scroller="true"]').children().contains(MEASURE_WITH_AGG)
            .parentsUntil('.MuiBox-root')
            .siblings('button')
            .click()

        cy.get(':nth-child(3)').get('.MuiAccordionSummary-content').contains('Measures').parent().parent()
            .get('.MuiAccordionDetails-root div[data-virtuoso-scroller="true"] div[data-test-id="virtuoso-item-list"] div')
            .should('not.exist');
    })
})
