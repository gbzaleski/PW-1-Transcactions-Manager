/*
 * Autor: Grzogorz B. Zaleski (418494)
 * Warszawa, 12-17 Grudnia 2020.
 */
package cp1.solution;
import cp1.base.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

// Implementacja menagera transakcji.
// Z komentarzy zgodnie z poleceniem usunieto polskie znaki.
// W celu lepsze czytelnosci operacje chronione
//      semaforem-mutexem graphGuard sa zapisane z wcieciem (tabem).
public class TranscactionManagerImpl implements  TransactionManager
{
    // Funkcja czasu
    private LocalTimeProvider timeProvider;

    // Czy dany watek ma odpolona transakcje.
    private Map<Thread, Boolean> transactionInProgress;
    // Poczatek transakcji na danym watku.
    private Map<Thread, Long> beginTime;
    // Czy dany watek ma abortowana transakcje.
    private Map<Thread, Boolean> transactionAborted;
    // Na jaki zasob czeka dany watek (brak = zaden)
    private Map<Thread, ResourceId> waitingForRes;

    // Dostep do zasobu po ID.
    private Map<ResourceId, Resource> mResources;
    // Blokady dostepnosci kazdego zasobu.
    private Map<ResourceId, Semaphore> resLock;
    // Stan danego zasobu (brak = zaden).
    private Map<ResourceId, Thread> resUser;

    // Ochrona relacji watki i zasobow.
    private Semaphore graphGuard;
    // Operacje wykonane przez dany watek (potrzebne do rollbackowania).
    private Map<Thread, ArrayList<OperationPair>> opsCompleted;

    // Sprawdzenie czy watek moze kontynuowac prace.
    private void checkStateToWork(Thread thread)
            throws NoActiveTransactionException,
            ActiveTransactionAborted
    {
        if (transactionInProgress.get(thread) == false)
            throw new NoActiveTransactionException();
        else if (transactionAborted.get(thread))
            throw new ActiveTransactionAborted();
    }

    // Pomocnicze funkcje do czytelnego uzywania semaforow - analogicznie do cwiczen/wykladow z przedmiotu.
    // Wziecie permisji.
    private void P(Semaphore sem) throws InterruptedException
    {
        sem.acquire();
    }
    // Oddanie permisji.
    private void V(Semaphore sem)
    {
        sem.release();
    }

    public TranscactionManagerImpl(Collection<Resource> resources, LocalTimeProvider timeProvider)
    {
        this.timeProvider = timeProvider;
        transactionInProgress = new ConcurrentHashMap<>();
        transactionAborted = new ConcurrentHashMap<>();
        waitingForRes = new ConcurrentHashMap<>();

        mResources = new ConcurrentHashMap<>();
        resUser = new ConcurrentHashMap<>();
        resLock = new ConcurrentHashMap<>();

        beginTime = new ConcurrentHashMap<>();
        opsCompleted = new ConcurrentHashMap<>();
        graphGuard = new Semaphore(1);

        for (var res: resources)
        {
            resLock.put(res.getId(), new Semaphore(1));
            mResources.put(res.getId(), res);
        }
    }

    @Override
    public void startTransaction()
            throws AnotherTransactionActiveException
    {
        Thread thread = Thread.currentThread();

        if (transactionInProgress.getOrDefault(thread, false))
            throw new AnotherTransactionActiveException();

        transactionInProgress.put(thread, true);
        transactionAborted.put(thread, false);
        beginTime.put(thread, timeProvider.getTime());
        opsCompleted.put(thread, new ArrayList<>());
    }

    private void checkCollision(Thread newThread, ResourceId currentRID)
    {
        Thread theLatestThread = newThread;
        long theLatestTime = beginTime.get(newThread);

        while (true)
        {
            // Poprawny ciag instukcji - skonczony.
            if (resUser.containsKey(currentRID) == false)
                return;
            var nextThread = resUser.get(currentRID);
            if (waitingForRes.containsKey(nextThread) == false)
                return;
            currentRID = waitingForRes.get(nextThread);

            // Aktualizacja potencjalnej transakcji do anulowania.
            if (theLatestTime < beginTime.get(nextThread)
                    || (theLatestTime == beginTime.get(nextThread) && theLatestThread.getId() < nextThread.getId()))
            {
                theLatestTime = beginTime.get(nextThread);
                theLatestThread = nextThread;
            }

            // Nastapil cykl.
            if (nextThread == newThread)
                break;
        }

        // Abortowanie najpozniejszej transakcji w cyklu.
        transactionAborted.put(theLatestThread, true);
        theLatestThread.interrupt();
    }

    @Override
    public void operateOnResourceInCurrentTransaction(ResourceId rid, ResourceOperation operation)
            throws NoActiveTransactionException,
            UnknownResourceIdException,
            ActiveTransactionAborted,
            ResourceOperationException,
            InterruptedException // Wywolywany przez dzialania na semaforach.
    {
        checkStateToWork(Thread.currentThread());
        if (mResources.containsKey(rid) == false)
            throw new UnknownResourceIdException(rid);

        Thread thread = Thread.currentThread();
        Resource res = mResources.get(rid);

        // Zajecie zasobu:
        P(graphGuard);
        if (resUser.containsKey(rid)) // zajety
        {
                waitingForRes.put(thread, rid);
                checkCollision(thread, rid);
            V(graphGuard);
                // Sprawdzenie na wypadek jesli kolizja wymagagala aborcji watku ktory jej szukal.
                checkStateToWork(thread);
                P(resLock.get(rid));

            P(graphGuard);
                waitingForRes.remove(thread);
                resUser.put(rid, thread);
            V(graphGuard);

        }
        else // wolny
        {
            V(graphGuard);

                 // W celu unikniecia potencjalnej utraty permita,
                //      P() wykonywane jest poza semaforem-mutexem.
                P(resLock.get(rid));

            P(graphGuard);
                resUser.put(rid, thread);
            V(graphGuard);
        }

        res.apply(operation);
        opsCompleted.get(thread).add(new OperationPair(rid, operation));
    }

    private void completeTransaction(boolean undo)
    {
        Thread thread = Thread.currentThread();
        var operations = opsCompleted.get(thread);

        if (operations.isEmpty())
            return;

        try
        {
            P(graphGuard);
        }
        catch (InterruptedException e)
        {
            // Jesli czekajacy na skonczenie transakcji watek zostanie przerwany to przerywamy dzialanie.
            return;
        }
            // Odwrotna kolejnosc zeby poprawnie sparowac odwrocenie dzialan.
            for (var i = operations.size() - 1; 0 <= i; i--)
            {
                var resID =  operations.get(i).getKey();
                var res =  mResources.get(resID);
                if (undo)
                    res.unapply(operations.get(i).getValue());
                resUser.remove(resID);

                V(resLock.get(resID));
            }
            waitingForRes.remove(thread);
        V(graphGuard);

        opsCompleted.put(thread, new ArrayList<>());
    }

    @Override
    public void commitCurrentTransaction()
            throws NoActiveTransactionException,
            ActiveTransactionAborted
    {
        checkStateToWork(Thread.currentThread());
        completeTransaction(false);
        transactionInProgress.put(Thread.currentThread(), false);
    }

    @Override
    public void rollbackCurrentTransaction()
    {
        completeTransaction(true);
        transactionInProgress.put(Thread.currentThread(), false);
    }

    @Override
    public boolean isTransactionActive()
    {
        return transactionInProgress.getOrDefault(Thread.currentThread(), false);
    }

    @Override
    public boolean isTransactionAborted()
    {
        return transactionAborted.getOrDefault(Thread.currentThread(), false);
    }
}
