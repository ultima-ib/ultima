import Board from './board';
import { authorQuoteMap } from './data';

export const Initial = () => {
    return <Board initial={authorQuoteMap} />
}
