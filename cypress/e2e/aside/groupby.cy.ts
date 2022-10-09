describe('Side bar', () => {
  beforeEach(() => {
    cy.visit('http://localhost:4173');
  })

  it('create new groupby', () => {
    cy.get(':nth-child(1)').contains('Group By').click();
    cy.get(':nth-child(3)').contains('COB').click();
    cy.get('.MuiListItem-padding > .MuiBox-root > .MuiListItem-root > .MuiButtonBase-root > .MuiListItemText-root > .MuiTypography-root').should('have.text', 'COB');
  })


  it('group by expands on field click', () => {
    cy.get('div[data-test-id="virtuoso-scroller"]').contains('TradeId').click();
    cy.get(':nth-child(3)').get('.virtuoso-grid-list').should('be.visible');
    cy.get(':nth-child(3)').get('.virtuoso-grid-list li').contains('TradeId').should('be.visible');
  })


  it('should remove grouped fields', () => {
    cy.get('div[data-test-id="virtuoso-scroller"]').contains('TradeId').click();
    cy.get(':nth-child(3)').get('.virtuoso-grid-list').should('be.visible');
    cy.get(':nth-child(3)').get('.virtuoso-grid-list li svg').click();
    cy.get(':nth-child(3)').get('.virtuoso-grid-list li').should('not.exist');
  })
})
