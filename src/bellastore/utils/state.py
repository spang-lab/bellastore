import os

from typing import List, Dict, Union, Tuple, Literal




StatusType = Literal["none", "valid", "hashed", "storage", "slide"]

class State():
    """
    Represents the states a scan can be in. It is intended to resemble a 'state machine', so once one
    state is not met, all following states need to also be not met.

    Init
    ----
        **Nothing** _None_

    Attributes:
        states (Dict[StatusType, bool]): a dict containing a boolean for each possible state

    Methods
    -------
        **is_valid**_()_: checks if the current state is valid\\
        **has_state**_(state)_: returns the state object's status of the input state\\
        **get_state**_()_: returns the highest state which is set to `True`\\
        **move_forward**_()_: moves the state one step forward (the lowest `False` will be set to `True`)
    """
    def __init__(self):
        # Since Python 3.7 dictionaries are ordered and guarantee to keep that during iterations
        self.states : Dict[StatusType, bool] = {
            "valid": False,
            "hashed": False,
            "storage": False,
            "slide": False
        }


    def is_valid(self) -> bool:
        """
        Check if the current state sequence is valid, meaning once a state is `False`,
        all subsequent states should also be `False`.

        Returns:
            is_valid (bool): True if the states are valid, False otherwise.
        """
        state_values = list(self.states.values())
        found_false = False

        for state in state_values:
            # This only is `True` if there has been a false and the current status in `True`
            if found_false and state:
                return False 
            if not state:
                found_false = True

        return True
    

    def has_state(self, state : StatusType) -> bool:
        """
        Checks if for this state instance the input state is `True`.
        The actual, maximal state might also be higher.
        This just checks if this specific condition is fulfilled.

        Args:
            state (StatusType): the state to be checked

        Returns:
            is_fulfilled (bool): indicating if the slide already has this status
        """
        if not self.is_valid():
            raise ValueError(f"The current state is not valid. Please fix this first.") 

        if state == "none":
            return True
        
        return self.states[state]


    def get_state(self) -> StatusType:
        """
        Get the name of the current state (the highest one).
        
        Returns:
            status (StatusType): The name of the current highest state that is True.
        """
        if not self.is_valid():
            raise ValueError(f"The current state is not valid. Please fix this first.") 
        
        max_state : StatusType = "none"
        
        # Iterate over the states as long as they are `True`
        for state, value in self.states.items():
            if value:
                max_state = state
            else:
                break

        return max_state


    def move_forward(self) -> None:
        """
        Move to the next available state in the progression by setting the next state to `True`.
        """
        if not self.is_valid():
            raise ValueError(f"The current state is not valid. Please fix this first.") 
        
        for state, value in self.states.items():
            if not value:
                self.states[state] = True
                return

        raise Warning(f"You've tried to move a slide from the maximum state further. This is not possible.")
    

    def __repr__(self) -> str:
        return f"Current States: {self.states}"

    


"""
- VALID: valid scanner vendor
- HASHED: slide included in ingress in `scans.sqlite`
- STORAGE: slide in storage in `scans.sqlite`, i.e. it is in (`/data/deep-learning/slides`) (and stored within correct hashed directory)
- SLIDE: slide.sqlite in storage folder, i.e. converted by pamly (this is where Paul's Slide() class takes off)
"""