describe('dark mode', () => {
    it("start with dark mode", () => {
        cy.visit('http://localhost:4173', {
            onBeforeLoad(win: Cypress.AUTWindow) {
                cy.stub(win, 'matchMedia')
                    .withArgs('(prefers-color-scheme: dark)')
                    .returns({
                        matches: true,
                        addListener: () => {}
                    })
            }
        });

        cy.get('body').should('have.css', 'background-color', 'rgb(18, 18, 18)')
    })

    it("start with light mode", () => {
        cy.visit('http://localhost:4173');

        cy.get('body').should('have.css', 'background-color', 'rgb(255, 255, 255)')
    })

    it('Toggle theme', function() {
        cy.visit('http://localhost:4173');
        cy.get('[data-testid="Brightness4Icon"]').click();
        cy.get('body').should('have.css', 'background-color', 'rgb(18, 18, 18)')
        cy.get('[data-testid="Brightness7Icon"]').click();
        cy.get('body').should('have.css', 'background-color', 'rgb(255, 255, 255)')
    });
})
