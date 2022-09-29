export type Id = string;

export interface Author {
  id: Id;
  name: string;
  avatarUrl: string;
  url: string;
}

export interface Quote {
  id: Id;
  content: string;
  author: Author;
}

export interface QuoteMap {
  [key: string]: Quote[];
}
