import React, { Component, ReactElement } from 'react';
import styled from '@emotion/styled';
import * as colors from './colors';
const grid = 8;
const borderRadius = 2;
import QuoteList from './quote-list';
import Title from './title';
import type { Quote } from './types';

const Container = styled.div`
  margin: ${grid}px;
  display: flex;
  flex-direction: column;
`;

interface HeaderProps {
  isDragging: boolean;
}

const Header = styled.div<HeaderProps>`
  display: flex;
  align-items: center;
  justify-content: center;
  border-top-left-radius: ${borderRadius}px;
  border-top-right-radius: ${borderRadius}px;
  background-color: ${({ isDragging }) =>
    isDragging ? colors.G50 : colors.N30};
  transition: background-color 0.2s ease;

  &:hover {
    background-color: ${colors.G50};
  }
`;

interface Props {
  title: string;
  quotes: Quote[];
  index: number;
  isScrollable?: boolean;
  useClone?: boolean;
}

export default class Column extends Component<Props> {
  render(): ReactElement {
    const title: string = this.props.title;
    const quotes: Quote[] = this.props.quotes;
    return (
        <Container>
          <Header isDragging={false}>
            <Title aria-label={`${title} quote list`}>
              {title}
            </Title>
          </Header>
          <QuoteList
              listId={title}
              listType="QUOTE"
              quotes={quotes}
              internalScroll={true}
              useClone={Boolean(this.props.useClone)}
          />
        </Container>
    );
  }
}
