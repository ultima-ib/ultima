import Board from './board';
import { authorQuoteMap, generateQuoteMap } from './data';

export const Initial = () => {
    return <Board initial={authorQuoteMap} />
}
