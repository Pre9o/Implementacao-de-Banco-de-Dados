package ibd.transaction.concurrency.rafaelcarneiropregardier;

import ibd.transaction.Transaction;
import ibd.transaction.concurrency.Item;
import ibd.transaction.concurrency.Lock;
import ibd.transaction.concurrency.LockBasedConcurrencyManager;
import ibd.transaction.instruction.Instruction;

import java.util.*;


public class RafaelCarneiroPregardierConcurrencyManager extends LockBasedConcurrencyManager {
    private final Map<Transaction, Set<Transaction>> Grafo = new HashMap<>();

    public RafaelCarneiroPregardierConcurrencyManager() throws Exception {
        super();
    }

    public boolean commit(Transaction t) throws Exception {
        boolean deuCerto = super.commit(t);
        if (deuCerto) {
            retirarDoGrafo(t);
        }
        return deuCerto;
    }

    public void abort(Transaction t) throws Exception {
        super.abort(t);
        retirarDoGrafo(t);
    }

    public Transaction addToQueue(Item item, Instruction instruction) {
        Transaction t = instruction.getTransaction();
        Lock l = new Lock(t, instruction.getMode());
        item.locks.add(l);

        atualizarGrafo(t, item, l);

        return verificarCiclo(t);
    }

    private void atualizarGrafo(Transaction t, Item item, Lock l) {
        item.locks.stream()
                .filter(lock -> podeAdicionarNoGrafo(t, l, lock))
                .forEach(lock -> adicionarAoGrafo(t, lock));
    }

    private boolean podeAdicionarNoGrafo(Transaction t, Lock l, Lock lock) {
        return !lock.transaction.equals(t) && (lock.mode != Instruction.READ || l.mode != Instruction.READ);
    }

    private void adicionarAoGrafo(Transaction t, Lock lock) {
        Grafo.computeIfAbsent(t, k -> new HashSet<>()).add(lock.transaction);
    }

    private dadosVerificarCiclo inicializarDadosDeVerificacao() {
        Set<Transaction> visitados = new HashSet<>();
        Set<Transaction> pilha = new HashSet<>();
        Set<Transaction> transacoesCiclo = new HashSet<>();

        return new dadosVerificarCiclo(visitados, pilha, transacoesCiclo);
    }

    private static class dadosVerificarCiclo {
        Set<Transaction> visitados;
        Set<Transaction> pilha;
        Set<Transaction> transacoesCiclo;

        dadosVerificarCiclo(Set<Transaction> visitados, Set<Transaction> pilha, Set<Transaction> transacoesCiclo) {
            this.visitados = visitados;
            this.pilha = pilha;
            this.transacoesCiclo = transacoesCiclo;
        }
    }

    private boolean ciclo(Transaction t, Set<Transaction> visitados, Set<Transaction> pilha, Set<Transaction> transacoesCiclo) {
        if (estaNaPilha(t, pilha, transacoesCiclo)) {
            return true;
        }
        if (foiVisitado(t, visitados)) {
            return false;
        }
        adicionarTransacaoNosVisitadosEPilha(t, visitados, pilha);

        if (cicloNosFilhos(t, visitados, pilha, transacoesCiclo)) {
            return true;
        }
        pilha.remove(t);
        return false;
    }

    private boolean estaNaPilha(Transaction t, Set<Transaction> pilha, Set<Transaction> transacoesCiclo) {
        if (pilha.contains(t)) {
            transacoesCiclo.addAll(pilha);
            return true;
        }
        return false;
    }

    private boolean foiVisitado(Transaction t, Set<Transaction> visitados) {
        return visitados.contains(t);
    }

    private void adicionarTransacaoNosVisitadosEPilha(Transaction t, Set<Transaction> visitados, Set<Transaction> pilha) {
        visitados.add(t);
        pilha.add(t);
    }

    private Transaction verificarCiclo(Transaction t) {
        dadosVerificarCiclo data = inicializarDadosDeVerificacao();

        if (ciclo(t, data.visitados, data.pilha, data.transacoesCiclo)) {
            return transacaoMaisNova(data.transacoesCiclo);
        }

        return null;
    }

    private boolean cicloNosFilhos(Transaction t, Set<Transaction> visitados, Set<Transaction> pilha, Set<Transaction> transacoesCiclo) {
        Set<Transaction> children = Grafo.getOrDefault(t, Collections.emptySet());
        for (Transaction child : children) {
            if (ciclo(child, visitados, pilha, transacoesCiclo)) {
                return true;
            }
        }
        return false;
    }

    private Transaction transacaoMaisNova(Set<Transaction> transactions) {
        return transactions.stream().max(Comparator.comparingInt(Transaction::getId)).orElse(null);
    }

    private void retirarDoGrafo(Transaction t) {
        Grafo.remove(t);
        for (Set<Transaction> transactions : Grafo.values()) {
            transactions.remove(t);
        }
    }
}
