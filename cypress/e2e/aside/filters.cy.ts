describe('Side bar: filters', () => {
    beforeEach(() => {
        cy.visit('http://localhost:4173');
    })
    it('Should start with one filter', function() {
        cy.contains('Filters')
            .parent()
            .siblings('.MuiPaper-root')
            .children('.MuiListItem-root')
            .should('have.length', 1);
    });

    it('Should add filter', function() {
        cy.get('button').contains('add filter').click();
        cy.get('button').contains('Filters')
            .parent()
            .siblings('.MuiPaper-root')
            .children('.MuiListItem-root')
            .should('have.length', 2);
    });

    it('Should add AND filter', function() {
        cy.get('button').contains('add and filter').click();
        cy.get('div').contains('Filters')
            .parent()
            .siblings('.MuiPaper-root')
            .children('.MuiListItem-root')
            .siblings('hr')

        cy.get('div').contains('Filters')
            .parent()
            .siblings('.MuiPaper-root')
            .children('button')
            .should('have.length', 3); // 3 because there's already also "add and filter" filter
    });

    it('Remove filter, empty the container', function() {
        cy.contains('Filters')
            .parent()
            .siblings('.MuiPaper-root')
            .children('.MuiListItem-root')
            .children()
            .each(($el, index) => {
                if (index === 1) {
                    cy.wrap($el).type('CO')
                    cy.get('.MuiAutocomplete-listbox [data-option-index="0"]').click()
                } else if (index === 2) {
                    cy.wrap($el).type('E')
                    cy.get('.MuiAutocomplete-listbox [data-option-index="0"]').click()
                } else if (index === 3) {
                    cy.wrap($el).click()
                    cy.get('.MuiAutocomplete-listbox [data-option-index="0"]').click()
                }
            })

        cy.contains('Filters')
            .parent()
            .siblings('.MuiPaper-root')
            .children('.MuiListItem-root')
            .children('button.MuiButtonBase-root')
            .click({ force: true })

        cy.contains('Filters')
            .parent()
            .siblings('.MuiPaper-root')
            .children('.MuiListItem-root')
            .should('have.length', 0);

        cy.get('div').contains('Filters')
            .parent()
            .siblings('.MuiPaper-root')
            .children('button')
            .should('have.length', 1); // "add and filter" buttons

    });
})
